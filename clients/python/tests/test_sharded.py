"""
一致性哈希与分布式分片客户端单元测试
"""

import time
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

    def test_fallback_with_exclude(self):
        nodes = ["node1:50051", "node2:50051"]
        ring = ConsistentHashRing(nodes)
        key = b"test_key_abc"
        initial_node = ring.get_node(key)

        # 排除 initial_node 后，必须 fallback 到另一个节点
        fallback_node = ring.get_node(key, exclude_nodes={initial_node})
        self.assertNotEqual(initial_node, fallback_node)
        self.assertIn(fallback_node, nodes)


class TestShardedDedupClient(unittest.TestCase):
    @patch("dedup_client.sharded.DedupClient")
    def test_sharded_routing_and_batch_ordering(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        mock_instances = {}

        def create_mock(addr, *args, **kwargs):
            m = MagicMock()
            m.addr = addr
            mock_instances[addr] = m
            return m

        mock_client_cls.side_effect = create_mock

        sharded = ShardedDedupClient(mock_nodes)
        self.assertEqual(len(sharded.clients), 2)

        def mock_batch_cas(items):
            return [f"result_for_{item[1].decode('utf-8')}" for item in items]

        for m in mock_instances.values():
            m.batch_check_and_set.side_effect = mock_batch_cas

        items = [
            ("crawler", f"url_{i}".encode("utf-8"), 1735689600000, None)
            for i in range(100)
        ]

        results = sharded.batch_check_and_set(items)

        self.assertEqual(len(results), 100)
        for i, res in enumerate(results):
            self.assertEqual(res, f"result_for_url_{i}")

        sharded.close()

    @patch("dedup_client.sharded.DedupClient")
    def test_retry_and_fallback(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        mock_instances = {}

        def create_mock(addr, *args, **kwargs):
            m = MagicMock()
            m.addr = addr
            mock_instances[addr] = m
            return m

        mock_client_cls.side_effect = create_mock

        sharded = ShardedDedupClient(
            mock_nodes,
            max_retries=2,
            retry_delay=0.01,
            backoff_factor=1.0,
        )

        node1 = mock_instances["127.0.0.1:50051"]
        node2 = mock_instances["127.0.0.1:50052"]

        # 让 node1 报错，node2 成功
        node1.check_and_set.side_effect = RuntimeError("Node 1 Down")
        node2.check_and_set.return_value = "inserted"

        # 执行单条查重，首选节点报错后顺延 fallback 到另一节点成功返回
        res = sharded.check_and_set("crawler", b"https://example.com/item1", 1000)
        self.assertEqual(res, "inserted")

        sharded.close()

    @patch("dedup_client.sharded.DedupClient")
    def test_circuit_breaker_cooling(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        mock_instances = {}

        def create_mock(addr, *args, **kwargs):
            m = MagicMock()
            m.addr = addr
            mock_instances[addr] = m
            return m

        mock_client_cls.side_effect = create_mock

        sharded = ShardedDedupClient(
            mock_nodes,
            max_retries=0,
            fail_threshold=2,
            cooling_time=1.0,
        )

        bad_node = "127.0.0.1:50051"
        good_node = "127.0.0.1:50052"
        mock_instances[bad_node].check_and_set.side_effect = RuntimeError("Failed")
        mock_instances[good_node].check_and_set.return_value = "inserted"

        # 连续失败 2 次，达到熔断阈值
        sharded._mark_failure(bad_node)
        sharded._mark_failure(bad_node)

        cooling = sharded._get_cooling_nodes()
        self.assertIn(bad_node, cooling)

        # 冷却期间，路由排除 bad_node
        target = sharded.ring.get_node(b"crawler:test", exclude_nodes=cooling)
        self.assertEqual(target, good_node)

        sharded.close()

    @patch("dedup_client.sharded.DedupClient")
    def test_batch_ignore_unavailable(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        mock_instances = {}

        def create_mock(addr, *args, **kwargs):
            m = MagicMock()
            m.addr = addr
            mock_instances[addr] = m
            return m

        mock_client_cls.side_effect = create_mock

        sharded = ShardedDedupClient(
            mock_nodes,
            max_retries=1,
            retry_delay=0.01,
        )

        # 让 50051 挂掉，50052 正常
        mock_instances["127.0.0.1:50051"].batch_check_and_set.side_effect = RuntimeError("Down")
        mock_instances["127.0.0.1:50052"].batch_check_and_set.return_value = ["duplicate"] * 10

        items = [
            ("crawler", f"item_{i}".encode("utf-8"), 1000, None)
            for i in range(10)
        ]

        # ignore_unavailable=True 时不抛异常，坏节点对应返回 None
        results = sharded.batch_check_and_set(items, ignore_unavailable=True)
        self.assertEqual(len(results), 10)
        # 其中部分结果为 None，部分为 duplicate
        self.assertTrue(any(r is None for r in results))

        sharded.close()

    @patch("dedup_client.sharded.DedupClient")
    def test_auth_token_forwarding(self, mock_client_cls):
        mock_nodes = ["127.0.0.1:50051", "127.0.0.1:50052"]
        sharded = ShardedDedupClient(mock_nodes, auth_token="secret_token_123")
        self.assertEqual(sharded.auth_token, "secret_token_123")
        for call_args in mock_client_cls.call_args_list:
            self.assertEqual(call_args.kwargs.get("auth_token"), "secret_token_123")
        sharded.close()


if __name__ == "__main__":
    unittest.main()
