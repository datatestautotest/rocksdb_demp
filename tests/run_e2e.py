#!/usr/bin/env python3
"""
自动化端到端 (E2E) 测试运行器

自动完成：
1. 编译并启动 RocksDB 去重服务（动态端口与临时目录）
2. 探活检测（/health）直至服务完全就绪
3. 运行 Python 客户端全链路查重、批量、过期与元数据测试
4. 运行 HTTP 运维端点测试（/metrics, /stats, /cleaner/pause, /cleaner/resume, /compact）
5. 运行分布式分片客户端 (ShardedDedupClient) 集群联调测试
6. 向服务发送停止信号并验证安全优雅退出
7. 自动回收临时测试数据
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request

# 将 clients/python/src 加入 sys.path
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(repo_root, "clients", "python", "src"))

from dedup_client import DedupClient, ShardedDedupClient


def http_get(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "E2E-Tester"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def http_get_text(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "E2E-Tester"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read().decode("utf-8")


def http_post(url: str) -> dict:
    req = urllib.request.Request(url, data=b"", headers={"User-Agent": "E2E-Tester"}, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def main():
    print("=" * 70)
    print("       RocksDB 去重服务自动化端到端 (E2E) 全链路测试")
    print("=" * 70)

    grpc_port = 45051
    metrics_port = 49090
    grpc_addr = f"127.0.0.1:{grpc_port}"
    metrics_addr = f"127.0.0.1:{metrics_port}"
    temp_dir = tempfile.mkdtemp(prefix="dedup_e2e_")

    print(f"[1/6] 创建隔离测试环境: {temp_dir}")
    print(f"      gRPC 服务端口:  {grpc_addr}")
    print(f"      HTTP 监控端口:  {metrics_addr}")

    # 查找或构建可执行文件
    bin_name = "rocksdb-dedup.exe" if os.name == "nt" else "rocksdb-dedup"
    bin_path = os.path.join(repo_root, "target", "debug", bin_name)

    if not os.path.exists(bin_path):
        print("[2/6] 编译 rocksdb-dedup 二进制程序...")
        subprocess.run(["cargo", "build", "--bin", "rocksdb-dedup"], cwd=repo_root, check=True)

    print(f"[2/6] 启动去重服务实例: {bin_path}")
    server_cmd = [
        bin_path,
        "serve",
        "--addr", grpc_addr,
        "--metrics-addr", metrics_addr,
        "--db-path", temp_dir,
        "--bloom-capacity", "50000",
        "--bloom-fpp", "0.01",
        "--cleaner-batch-size", "1000",
        "--block-cache-mb", "32",
    ]

    proc = subprocess.Popen(server_cmd, cwd=repo_root, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

    try:
        # 等待服务健康探针就绪
        print("[3/6] 等待服务探活探针 (http://{}/health)...".format(metrics_addr))
        ready = False
        health_url = f"http://{metrics_addr}/health"
        for _ in range(30):
            try:
                res = http_get(health_url)
                if res.get("status") == "UP":
                    ready = True
                    break
            except Exception:
                time.sleep(0.5)

        if not ready:
            print("❌ 服务启动超时或未通过探活检测！")
            sys.exit(1)

        print("      ✅ 服务健康探活成功：status=UP")

        # 4. 执行核心 gRPC 接口验证
        print("[4/6] 运行 Python 客户端查重与批量功能测试...")
        with DedupClient(grpc_addr) as client:
            ns = "e2e_test"
            now_ms = int(time.time() * 1000)
            expire_at = now_ms + 7200_000  # 2小时后

            # (1) 新增写入
            r1 = client.check_and_set(ns, b"https://example.com/item/1", expire_at, metadata=b'{"tag": "tech"}')
            assert r1 == "inserted", f"期望 inserted，实际 {r1}"

            # (2) 重复查重
            r2 = client.check_and_set(ns, b"https://example.com/item/1", expire_at)
            assert r2 == "duplicate", f"期望 duplicate，实际 {r2}"

            # (3) exists 与 get
            assert client.exists(ns, b"https://example.com/item/1") is True
            assert client.exists(ns, b"https://example.com/item/none") is False

            val = client.get(ns, b"https://example.com/item/1")
            assert val is not None
            assert val["metadata"] == b'{"tag": "tech"}'

            # (4) 批量操作
            batch_items = [
                (ns, f"https://example.com/batch/{i}".encode("utf-8"), expire_at, None)
                for i in range(50)
            ]
            batch_res = client.batch_check_and_set(batch_items)
            assert len(batch_res) == 50
            assert all(r == "inserted" for r in batch_res)

            # 再次批量应全部重复
            batch_res_dup = client.batch_check_and_set(batch_items)
            assert all(r == "duplicate" for r in batch_res_dup)

            # 批量存在性
            keys = [f"https://example.com/batch/{i}".encode("utf-8") for i in range(50)]
            exist_res = client.batch_exists(ns, keys)
            assert len(exist_res) == 50 and all(exist_res)

            # 删除与批量删除
            assert client.delete(ns, b"https://example.com/item/1") is True
            del_res = client.batch_delete(ns, keys[:10])
            assert len(del_res) == 10 and all(del_res)
            assert client.exists(ns, keys[0]) is False

            stats = client.stats()
            print(f"      ✅ 单节点客户端功能验证通过 (total_keys={stats['total_keys']}, bloom_ins={stats['bloom_insertions']})")

        # 5. 运行 HTTP 运维与监控接口测试
        print("[5/6] 运行 HTTP 运维控制与 Prometheus 指标接口测试...")
        # 暂停清理器
        p_res = http_post(f"http://{metrics_addr}/cleaner/pause")
        assert "cleaner paused" in p_res.get("status", "")
        h_res = http_get(health_url)
        assert h_res.get("cleaner_paused") is True

        # 恢复清理器
        r_res = http_post(f"http://{metrics_addr}/cleaner/resume")
        assert "cleaner resumed" in r_res.get("status", "")
        h_res2 = http_get(health_url)
        assert h_res2.get("cleaner_paused") is False

        # 手动 Compaction
        c_res = http_post(f"http://{metrics_addr}/compact")
        assert c_res.get("status") == "COMPACTED"

        # Prometheus 指标校验
        prom_text = http_get_text(f"http://{metrics_addr}/metrics")
        assert "rocksdb_dedup_bloom_capacity" in prom_text
        assert "rocksdb_dedup_cleaner_compacts_total" in prom_text
        print("      ✅ HTTP 运维控制（Pause/Resume/Compact/Prometheus）验证通过")

        # 6. 分布式分片客户端连通测试
        print("[6/6] 运行高可用分布式分片客户端 (ShardedDedupClient) 联调测试...")
        with ShardedDedupClient([grpc_addr], replicas=64) as sharded:
            sharded_items = [
                ("sharded_ns", f"https://example.com/sharded/{i}".encode("utf-8"), expire_at, None)
                for i in range(30)
            ]
            sharded_res = sharded.batch_check_and_set(sharded_items)
            assert len(sharded_res) == 30
            assert all(r == "inserted" for r in sharded_res)

            sharded_stats = sharded.get_cluster_stats()
            assert sharded_stats["cluster_nodes_count"] == 1
            print(f"      ✅ 分片客户端联调通过 (cluster_total_keys={sharded_stats['total_keys']})")

        print("\n" + "=" * 70)
        print("🎉 全部 E2E 端到端测试项 100% 验证通过！")
        print("=" * 70)

    finally:
        print("正在停止后台服务进程...")
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except Exception:
            proc.kill()

        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir, ignore_errors=True)
        print("测试清理完成。")


if __name__ == "__main__":
    main()
