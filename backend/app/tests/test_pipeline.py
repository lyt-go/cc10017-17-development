"""跨模块数据链路测试：覆盖6条核心链路的真实数据流转断言。"""
from __future__ import annotations

import unittest
from typing import Any

from app.pipeline_engine import pipeline_engine
from app.services.topic_registry import TopicRegistryService
from app.services.flink_job import FlinkJobService
from app.services.change_capture import ChangeCaptureService
from app.services.stream_quality import StreamQualityService
from app.store import store


class TestIngestChain(unittest.TestCase):
    """实时接入链：主题注册→Kafka接入→Schema演进→Flink计算"""

    def setUp(self) -> None:
        self.topic_svc = TopicRegistryService()
        self.flink_svc = FlinkJobService()

    def test_topic_upstream_validation(self) -> None:
        """Flink作业启动时必须校验上游主题已上线。"""
        # 草稿主题
        topic, _ = self.topic_svc.create_entry({
            "主题编号": "TOPI-TEST1", "主题名称": "测试主题", "消息格式": "JSON"
        })
        self.assertEqual(topic["status"], "草稿")

        # Flink引用该主题但主题未上线，应拒绝启动
        flink, _ = self.flink_svc.create_entry({
            "任务编号": "FLIN-TEST1", "消费主题": "TOPI-TEST1", "转换逻辑": "test"
        })
        entry, msg = self.flink_svc.run_action(flink["id"], "启动作业")
        self.assertIsNone(entry)
        self.assertIn("须先上线", msg)

    def test_topic_online_triggers_downstream_check(self) -> None:
        """主题上线时自动触发下游就绪检查。"""
        topic, _ = self.topic_svc.create_entry({
            "主题编号": "TOPI-TEST2", "主题名称": "测试主题2", "消息格式": "JSON"
        })
        entry, msg = self.topic_svc.run_action(topic["id"], "确认上线")
        self.assertIsNotNone(entry)
        self.assertEqual(entry["status"], "已上线")
        # 下游检查标记应存在
        self.assertIn("_downstream_ready", entry)

    def test_ingest_chain_full_status(self) -> None:
        """查询完整接入链状态。"""
        status = pipeline_engine.ingest_chain_status("TOPI-0001")
        self.assertTrue(status["topic"]["exists"])
        self.assertTrue(status["kafka_connector"]["exists"])
        self.assertTrue(status["schema_evolve"]["exists"])
        self.assertTrue(status["flink_job"]["exists"])
        # 种子数据中TOPI-0001的链路是就绪的
        self.assertTrue(status["chain_ready"])


class TestCDCChain(unittest.TestCase):
    """CDC入湖链：CDC捕获→UPSERT合并→湖分区→Iceberg管理"""

    def setUp(self) -> None:
        self.cdc_svc = ChangeCaptureService()

    def test_cdc_start_requires_target_topic_online(self) -> None:
        """CDC启动时必须校验目标主题已上线。"""
        # 尝试为草稿主题启动CDC（种子中CHAN-0003对应TOPI-0003是草稿）
        cdc = store.find("change_capture", 3)
        self.assertEqual(cdc["status"], "草稿")
        entry, msg = self.cdc_svc.run_action(3, "启动捕获")
        self.assertIsNone(entry)
        self.assertIn("未上线", msg)

    def test_cdc_start_requires_upsert_policy(self) -> None:
        """CDC启动时必须校验源表有合并策略。"""
        # 种子中CHAN-0001对应orders表有UPSE-0001
        entry, msg = self.cdc_svc.run_action(1, "启动捕获")
        # 它已经是运行中，重新启动应成功
        self.assertIsNotNone(entry)

    def test_cdc_upsert_simulation(self) -> None:
        """CDC增量合并模拟产生真实产物。"""
        result = pipeline_engine.run_cdc_upsert_simulation(1)
        self.assertTrue(result["ok"])
        self.assertIn("artifacts", result)
        self.assertGreater(result["merged_count"], 0)
        # 产物应写入artifact表
        artifacts = store.rows("upsert_merge_artifacts")
        self.assertGreater(len(artifacts), 0)

    def test_partition_apply_to_iceberg(self) -> None:
        """分区策略应用到Iceberg表产生元数据产物。"""
        result = pipeline_engine.apply_partition_to_iceberg("orders")
        self.assertTrue(result["ok"])
        self.assertIn("spec", result)
        specs = store.rows("iceberg_partition_specs")
        self.assertTrue(any(s.get("table_name") == "orders" for s in specs))

    def test_cdc_chain_status(self) -> None:
        """CDC入湖链全链路状态查询。"""
        status = pipeline_engine.cdc_chain_status("mysql-prod-1", "orders")
        self.assertTrue(status["cdc"]["exists"])
        self.assertTrue(status["upsert_merge"]["exists"])
        self.assertTrue(status["lake_partition"]["exists"])
        self.assertTrue(status["iceberg_table"]["exists"])
        self.assertTrue(status["chain_ready"])


class TestQualityChain(unittest.TestCase):
    """质量容错链：水位追踪→流式质量→死信处理"""

    def setUp(self) -> None:
        self.quality_svc = StreamQualityService()

    def test_quality_chain_check(self) -> None:
        """质量链健康检查。"""
        status = pipeline_engine.quality_chain_check("TOPI-0001")
        self.assertTrue(status["watermark"]["exists"])
        self.assertTrue(status["stream_quality"]["exists"])
        self.assertIn("quality_alert", status)

    def test_quality_inspection_produces_result(self) -> None:
        """质量巡检产生检查结果产物。"""
        result = pipeline_engine.run_quality_inspection("TOPI-0001")
        self.assertTrue(result["ok"])
        self.assertIn("result", result)
        results = store.rows("quality_inspection_results")
        self.assertTrue(any(r.get("topic") == "TOPI-0001" for r in results))

    def test_quality_rule_activation_checks_watermark(self) -> None:
        """质量规则生效时触发水位检查。"""
        # 创建新规则并生效
        entry, _ = self.quality_svc.create_entry({
            "规则编号": "STRE-TEST1", "监控主题": "TOPI-0001", "校验类型": "完整性校验",
            "告警阈值": "5", "采样比例": "0.1", "检测窗口": "1min",
            "阻断动作": "告警", "规则版本": "v1"
        })
        # TOPI-0001的水位延迟记录数是12，阈值5，应触发告警
        result, msg = self.quality_svc.run_action(entry["id"], "标记生效")
        self.assertIsNotNone(result)
        self.assertEqual(result["status"], "已生效")
        # 应产生死信
        dead_letters = [r for r in store.rows("dead_letter") if r.get("原始主题") == "TOPI-0001"]
        self.assertGreater(len(dead_letters), 0)


class TestConsumptionChain(unittest.TestCase):
    """数据消费链：流式血缘→物化视图→下沉分发"""

    def test_consumption_chain_status(self) -> None:
        """消费链全链路状态查询。"""
        status = pipeline_engine.consumption_chain_status("TOPI-0001")
        self.assertTrue(status["lineage"]["exists"])
        self.assertTrue(status["materialized_view"]["exists"])
        self.assertTrue(status["sink_dispatch"]["exists"])
        self.assertTrue(status["chain_ready"])

    def test_refresh_materialized_triggers_dispatch(self) -> None:
        """血缘刷新驱动物化视图并触发下沉分发队列。"""
        result = pipeline_engine.refresh_materialized_from_lineage(1)
        self.assertTrue(result["ok"])
        self.assertIn("refreshed", result)
        # 应产生刷新日志
        logs = store.rows("materialized_refresh_log")
        self.assertTrue(any(l.get("lineage_id") == 1 for l in logs))
        # 应触发下沉队列
        queue = store.rows("sink_dispatch_queue")
        self.assertTrue(any(q.get("sink_rule_id") == 1 for q in queue))


class TestMaintenanceChain(unittest.TestCase):
    """维护链：流式补数→合并清理→数据生命周期"""

    def test_maintenance_chain_status(self) -> None:
        """维护链全链路状态查询。"""
        status = pipeline_engine.maintenance_chain_status("orders")
        self.assertTrue(status["backfill"]["exists"])
        self.assertTrue(status["compact_policy"]["exists"])
        self.assertTrue(status["ttl_policy"]["exists"])
        # 种子中orders的backfill是已完成
        self.assertTrue(status["chain_ready"])

    def test_backfill_triggers_compact_and_ttl(self) -> None:
        """补数完成后触发合并清理与生命周期。"""
        result = pipeline_engine.run_backfill_then_compact(1)
        self.assertTrue(result["ok"])
        self.assertIn("result", result)
        # 应产生合并产物
        compacts = store.rows("compact_results")
        self.assertTrue(any(c.get("backfill_id") == 1 for c in compacts))
        # 应产生生命周期应用记录
        ttls = store.rows("ttl_applied_log")
        self.assertTrue(any(t.get("table") == "orders" for t in ttls))


class TestMetricsChain(unittest.TestCase):
    """实时指标链：消费主题注册与Flink计算结果"""

    def test_metrics_computes_from_topics_and_flink(self) -> None:
        """实时指标计算消费主题和Flink作业数据。"""
        result = pipeline_engine.compute_realtime_metrics(1)
        self.assertTrue(result["ok"])
        self.assertIn("result", result)
        res = result["result"]
        self.assertIn("source_topics", res)
        self.assertIn("source_flink_jobs", res)
        self.assertGreaterEqual(res["source_topics"], 0)
        self.assertGreaterEqual(res["source_flink_jobs"], 0)
        # 产物写入
        metrics = store.rows("realtime_metric_results")
        self.assertTrue(any(m.get("metric_id") == 1 for m in metrics))


class TestFullLineageGraph(unittest.TestCase):
    """全平台血缘图"""

    def test_full_lineage_has_nodes_and_edges(self) -> None:
        """全平台血缘图包含节点和边。"""
        graph = pipeline_engine.build_full_lineage_graph()
        self.assertIn("nodes", graph)
        self.assertIn("edges", graph)
        self.assertGreater(len(graph["nodes"]), 0)
        # 应有主题到Flink的边
        topic_nodes = {n["name"]: n["id"] for n in graph["nodes"] if n["type"] == "topic"}
        flink_nodes = {n["name"]: n["id"] for n in graph["nodes"] if n["type"] == "job"}
        self.assertGreater(len(topic_nodes), 0)
        self.assertGreater(len(flink_nodes), 0)

    def test_pipeline_overview(self) -> None:
        """管道总览包含各链路统计。"""
        overview = pipeline_engine.pipeline_overview()
        self.assertIn("chains", overview)
        self.assertIn("artifact_counts", overview)
        self.assertIn("realtime_ingest", overview["chains"])
        self.assertIn("cdc_to_lake", overview["chains"])

    def test_overview_module_count_stays_18(self) -> None:
        """内部支撑表不冒充业务模块，18 个既有业务模块仍全部在列。"""
        overview = store.overview()
        names = {row["name"] for row in overview["modules"]}
        expected = {
            "topic_registry", "flink_job", "kafka_connector", "schema_evolve",
            "upsert_merge", "lake_partition", "stream_quality", "iceberg_table",
            "watermark_track", "dead_letter", "materialized_view", "change_capture",
            "compact_policy", "stream_lineage", "backfill_stream", "sink_dispatch",
            "ttl_policy", "realtime_metrics",
        }
        self.assertEqual(len(expected), 18)
        self.assertTrue(expected.issubset(names))
        self.assertNotIn("watermark_partition_samples", names)
        artifacts = overview["artifacts"]
        self.assertIn("watermark_offset_reports", artifacts)
        self.assertIn("quality_partition_alerts", artifacts)


class TestCrossModuleConstraints(unittest.TestCase):
    """跨模块约束与状态机校验"""

    def test_topic_status_machine(self) -> None:
        """主题状态机不允许非法回退。"""
        svc = TopicRegistryService()
        topic, _ = svc.create_entry({
            "主题编号": "TOPI-SM1", "主题名称": "状态机测试", "消息格式": "JSON"
        })
        # 草稿→已上线
        entry, _ = svc.run_action(topic["id"], "确认上线")
        self.assertEqual(entry["status"], "已上线")
        # 已上线→草稿 应失败
        entry2, msg = svc.run_action(topic["id"], "提交校验")
        self.assertIsNone(entry2)
        self.assertIn("回退", msg)

    def test_cdc_creates_capture_artifact(self) -> None:
        """CDC启动产生捕获产物。"""
        svc = ChangeCaptureService()
        # 先创建一个新的CDC配置关联已上线主题
        cdc, _ = svc.create_entry({
            "配置编号": "CHAN-TEST1", "源数据库": "mysql-test", "源表名": "orders",
            "目标主题": "TOPI-0001"
        })
        entry, _ = svc.run_action(cdc["id"], "启动捕获")
        self.assertIsNotNone(entry)
        artifacts = svc.get_capture_artifacts(cdc["id"])
        self.assertGreater(len(artifacts), 0)


if __name__ == "__main__":
    unittest.main()
