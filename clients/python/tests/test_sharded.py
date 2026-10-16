"""
一致性哈希与分布式分片客户端单元测试
"""

import unittest
from unittest.mock import MagicMock, patch

from dedup_client import ConsistentHashRing, ShardedDedupClient


class TestConsistentHashRing(unittest.TestCase):
    def test_empty_ring(self):
        ring = ConsistentHashRing()
        with self.assertRaises(RuntimeError):
            ring.get_node(b"any_key")

    def test_single_node(self):
        ring = ConsistentHashRing(["10.0.0.1:50051"])
        for i in range(100):
            node = ring.get_node(f"key_{i}".encode("utf-8"))
            self.assertEqual(node, "10.0.0.1:50051")

    def test_consistent_routing(self):
        nodes = ["10.0.0.1:50051", "10.0.0.2:50051", "10.0.0.3:50051"]
        ring = ConsistentHashRing(nodes)

        # 相同的 key 多次查询必须定位到同一节点
        sample_keys = [f"https://example.com/item/{i}".encode("utf-8") for i in range(200)]
        first_mapping = [ring.get_node(k) for k in sample_keys]
        second_mapping = [ring.get_node(k) for k in sample_keys]
        self.assertEqual(first_mapping, second_mapping)

    def test_hash_distribution_uniformity(self):
        # 3 个节点，3000 个随机 URL，每个节点占比应在 25% ~ 42% 之间（虚拟节点平衡效果）
        nodes = ["node1:50051", "node2:50051", "node3:50051"]
        ring = ConsistentHashRing(nodes, replicas=160)

        counts = {n: 0 for n in nodes}
        total = 3000
        for i in range(total):
            k = f"crawler:https://news.sina.com.cn/article/{i * 7 + 13}.html".encode("utf-8")
            n = ring.get_node(k)
            counts[n] += 1

        for n, cnt in counts.items():
            ratio = cnt / total
            self.assertGreater(ratio, 0.25, f"{n} distribution too low: {ratio:.3f}")
            self.assertLess(ratio, 0.42, f"{n} distribution too high: {ratio:.3f}")

    def test_add_remove_node(self):
        nodes = ["node1:50051", "node2:50051"]
        ring = ConsistentHashRing(nodes)
        self.assertEqual(len(ring.nodes), 2)

        ring.add_node("node3:50051")
        self.assertEqual(len(ring.nodes), 3)

        ring.remove_node("node2:50051")
        self.assertEqual(len(ring.nodes), 2)
        self.assertNotIn("node2:50051", ring.nodes)


class TestShardedDedupClient(unittest.TestCase):
    @patch("dedup_client.sharded.DedupClient")
    def test_sharded_routing_and_batch_ordering(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        mock_instances = {}

        def create_mock(addr, timeout=5.0):
            m = MagicMock()
            m.addr = addr
            mock_instances[addr] = m
            return m

        mock_client_cls.side_effect = create_mock

        sharded = ShardedDedupClient(mock_nodes)
        self.assertEqual(len(sharded.clients), 2)

        # 配置 mock 的 batch_check_and_set 行为：对于输入列表，为每个条目返回对应 tag
        def mock_batch_cas(items):
            # 返回 mock 标记
            return [f"result_for_{item[1].decode('utf-8')}" for item in items]

        for m in mock_instances.values():
            m.batch_check_and_set.side_effect = mock_batch_cas

        # 构造 100 条待去重项目
        items = [
            ("crawler", f"url_{i}".encode("utf-8"), 1735689600000, None)
            for i in range(100)
        ]

        # 执行分布式批量检查
        results = sharded.batch_check_and_set(items)

        # 验证返回结果严格按照输入下标保序
        self.assertEqual(len(results), 100)
        for i, res in enumerate(results):
            self.assertEqual(res, f"result_for_url_{i}")

        sharded.close()


if __name__ == "__main__":
    unittest.main()
