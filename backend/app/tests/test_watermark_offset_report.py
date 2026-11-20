"""双水位偏移报告测试：偏移来源、受影响分区与检查中断后的重试补齐。"""
from __future__ import annotations

import unittest

from app.services.stream_quality import StreamQualityService
from app.services.watermark_track import WatermarkTrackService
from app.store import store

TOPIC = "TOPI-0001"


class TestWatermarkOffsetReport(unittest.TestCase):
    def setUp(self) -> None:
        store.rows("watermark_offset_reports").clear()
        store.rows("quality_check_results").clear()
        store.rows("quality_partition_alerts").clear()
        self.svc = WatermarkTrackService()

    def test_report_keeps_source_and_affected_partitions(self) -> None:
        """报告必须同时给出偏移来源和受影响分区。"""
        report, message = self.svc.generate_offset_report(TOPIC, check_version="v-test-1")
        self.assertIsNotNone(report)
        self.assertEqual(report["状态"], "完整")
        self.assertIn(report["偏移来源"], ("事件时间", "处理时间"))
        self.assertTrue(report["受影响分区"])
        self.assertEqual(report["来源批次"], "BATCH-20261116-1005")
        # 两条水位各自点名的分区都要留在报告里，才能说明取舍。
        self.assertIn("事件时间滞后分区", report)
        self.assertIn("处理时间滞后分区", report)
        self.assertIn("分区 2 在同一检查窗口内两条水位反向移动", report["来源判定"])
        self.assertEqual(report["检查版本"], "v-test-1")
        self.assertIn("WREP", message)

    def test_two_watermarks_pick_different_partitions(self) -> None:
        """两条水位点名的滞后分区不同：事件时间判分区1，处理时间判分区2。"""
        report, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-2")
        event_partitions = report["事件时间滞后分区"]
        process_partitions = report["处理时间滞后分区"]
        # 分区1 事件时间停滞、处理时间前进；分区2 事件时间前进、处理时间回退（反向移动）。
        self.assertEqual(event_partitions, ["1"])
        self.assertEqual(process_partitions, ["2"])
        self.assertNotEqual(event_partitions, process_partitions)
        # 两边各一个时按滞后合计取舍，报告只给一个版本。
        self.assertEqual(report["偏移来源"], "处理时间")
        self.assertEqual(report["受影响分区"], process_partitions)

    def test_forced_source_changes_affected_partitions(self) -> None:
        """同一检查窗口下改用另一条水位定偏移，受影响分区随来源变化。"""
        by_event, _ = self.svc.generate_offset_report(TOPIC, check_version="v-force-event", source="事件时间")
        by_process, _ = self.svc.generate_offset_report(TOPIC, check_version="v-force-process", source="处理时间")
        self.assertEqual(by_event["偏移来源"], "事件时间")
        self.assertEqual(by_process["偏移来源"], "处理时间")
        self.assertEqual(by_event["受影响分区"], ["1"])
        self.assertEqual(by_process["受影响分区"], ["2"])
        # 两条水位各自的判定在两次报告里一致，只取决于检查窗口。
        self.assertEqual(by_event["事件时间滞后分区"], by_process["事件时间滞后分区"])
        self.assertEqual(by_event["处理时间滞后分区"], by_process["处理时间滞后分区"])

    def test_rerun_same_version_keeps_source(self) -> None:
        """重跑同一检查版本返回同一份报告，偏移来源不变。"""
        first, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-3")
        second, message = self.svc.generate_offset_report(TOPIC, check_version="v-test-3", source="处理时间")
        self.assertEqual(first["报告编号"], second["报告编号"])
        self.assertEqual(first["偏移来源"], second["偏移来源"])
        self.assertEqual(len(store.rows("watermark_offset_reports")), 1)
        self.assertIn("已有完整报告", message)

    def test_interrupted_check_keeps_previous_complete_report(self) -> None:
        """检查中断时保留上一份完整报告，并记下缺失分区。"""
        complete, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-4")
        self.assertEqual(complete["状态"], "完整")

        partial, message = self.svc.generate_offset_report(TOPIC, check_version="v-test-5", interrupt_after=1)
        self.assertEqual(partial["状态"], "不完整")
        self.assertFalse(partial["已确认偏移"])
        self.assertEqual(partial["缺失分区"], ["1", "2"])
        self.assertEqual(partial["保留报告"], complete["报告编号"])
        self.assertIn("检查中断", message)
        # 上一份完整报告仍在，当前业务版本仍然指向它。
        current = self.svc.get_current_report(TOPIC)
        self.assertEqual(current["报告编号"], complete["报告编号"])

    def test_retry_fills_missing_partitions_only(self) -> None:
        """中断后重试补齐后续项，已确认分区不重算。"""
        complete, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-6")
        partial, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-7", interrupt_after=1)
        confirmed_before = {item["分区编号"]: item for item in partial["分区明细"]}

        filled, message = self.svc.retry_offset_report(partial["id"])
        self.assertEqual(filled["状态"], "完整")
        self.assertTrue(filled["已确认偏移"])
        self.assertEqual(filled["补齐分区"], ["1", "2"])
        self.assertEqual(filled["缺失分区"], [])
        self.assertEqual(filled["报告编号"], partial["报告编号"])
        self.assertIn("补齐", message)
        # 已确认分区的明细保持原样，只追加缺失分区。
        for item in filled["分区明细"]:
            if item["分区编号"] in confirmed_before:
                self.assertEqual(item, confirmed_before[item["分区编号"]])
        self.assertEqual([item["分区编号"] for item in filled["分区明细"]], ["0", "1", "2"])
        self.assertEqual(filled["偏移来源"], complete["偏移来源"])
        # 中断期只看部分分区时曾按事件时间记录，补齐后改判，留痕可查。
        self.assertEqual(filled["中断期偏移来源"], "事件时间")
        self.assertIn("中断期", filled["来源判定"])
        self.assertNotEqual(filled["偏移来源"], filled["中断期偏移来源"])
        # 补齐后当前业务版本切到新报告。
        self.assertEqual(self.svc.get_current_report(TOPIC)["报告编号"], partial["报告编号"])

    def test_retry_cannot_flip_source(self) -> None:
        """同一个检查版本不能因为重试改判偏移来源。"""
        _, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-8", interrupt_after=1)
        partial = self.svc.get_report_by_version(TOPIC, "v-test-8")
        filled, message = self.svc._fill_missing(
            dict(store.find("watermark_offset_reports", partial["id"])),
            self.svc._partition_samples(TOPIC),
            "处理时间",
        )
        self.assertEqual(filled["偏移来源"], partial["偏移来源"])
        self.assertIn("不能改判", message)

    def test_recompute_all_starts_new_version(self) -> None:
        """重算全部分区会覆盖已确认偏移，因此另起检查版本，原报告保留。"""
        first, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-9")
        second, message = self.svc.generate_offset_report(TOPIC, check_version="v-test-9", scope="全部")
        self.assertNotEqual(first["报告编号"], second["报告编号"])
        self.assertEqual(second["替代报告"], first["报告编号"])
        self.assertEqual(first["状态"], "完整")
        self.assertIn("偏移来源", message)
        self.assertEqual(len(store.rows("watermark_offset_reports")), 2)

    def test_retry_reports_on_complete_report(self) -> None:
        """对已完整报告重试只返回原报告，不产生新版本。"""
        report, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-10")
        again, message = self.svc.retry_offset_report(report["id"])
        self.assertEqual(again["报告编号"], report["报告编号"])
        self.assertIn("无需补齐", message)
        self.assertEqual(len(store.rows("watermark_offset_reports")), 1)

    def test_overview_uses_current_business_version(self) -> None:
        """概览与列表、详情读同一份当前业务版本报告。"""
        first, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-11")
        second, _ = self.svc.generate_offset_report(TOPIC, check_version="v-test-12", interrupt_after=1)
        overview = self.svc.report_overview()
        view = next(item for item in overview["views"] if item["数据主题"] == TOPIC)
        self.assertEqual(view["当前报告编号"], first["报告编号"])
        self.assertEqual(view["当前检查版本"], "v-test-11")
        self.assertEqual(view["未完成版本数"], 1)
        self.assertEqual(view["当前偏移来源"], first["偏移来源"])
        self.assertEqual(view["当前受影响分区"], first["受影响分区"])
        detail = self.svc.get_report(first["id"])
        self.assertEqual(detail, view["当前报告"])

        # 重试补齐后，概览、当前业务版本与详情一起切换。
        self.svc.retry_offset_report(second["id"])
        overview = self.svc.report_overview()
        view = next(item for item in overview["views"] if item["数据主题"] == TOPIC)
        self.assertEqual(view["当前报告编号"], second["报告编号"])
        self.assertEqual(self.svc.get_current_report(TOPIC), view["当前报告"])


class TestQualityBindsOffsetReport(unittest.TestCase):
    def setUp(self) -> None:
        store.rows("watermark_offset_reports").clear()
        store.rows("quality_check_results").clear()
        store.rows("quality_partition_alerts").clear()
        store.rows("dead_letter").clear()
        self.quality_svc = StreamQualityService()
        self.watermark_svc = WatermarkTrackService()

    def _rule(self, threshold: str) -> dict:
        entry, _ = self.quality_svc.create_entry({
            "规则编号": "STRE-OFFSET", "监控主题": TOPIC, "校验类型": "延迟校验",
            "告警阈值": threshold, "采样比例": "0.1", "检测窗口": "5min",
            "阻断动作": "告警", "规则版本": "v1",
        })
        return entry

    def test_quality_alert_binds_to_same_report(self) -> None:
        """质量告警绑定同一次水位检查，并保留报告编号与来源批次。"""
        entry = self._rule("100")
        result, _ = self.quality_svc.run_action(entry["id"], "标记生效")
        self.assertIsNotNone(result)
        check = entry["_last_check"]

        report = self.watermark_svc.get_report_by_version(TOPIC, check["check_version"])
        self.assertIsNotNone(report)
        self.assertEqual(check["report_code"], report["报告编号"])
        self.assertEqual(check["batch"], report["来源批次"])
        self.assertEqual(check["offset_source"], report["偏移来源"])
        self.assertEqual(check["affected_partitions"], report["受影响分区"])

        alerts = self.quality_svc.list_alerts(TOPIC)
        self.assertTrue(alerts)
        # 阈值100只让延迟156的分区2超限；分区1延迟88低于阈值。
        self.assertEqual({alert["分区编号"] for alert in alerts}, {"2"})
        for alert in alerts:
            self.assertEqual(alert["报告编号"], report["报告编号"])
            self.assertEqual(alert["来源批次"], report["来源批次"])
            self.assertEqual(alert["检查版本"], check["check_version"])
            self.assertEqual(alert["状态"], "待处置")

    def test_recheck_reuses_same_version(self) -> None:
        """两条规则核对同一检查版本时读到同一份报告，不重复生成。"""
        first = self._rule("50")
        self.quality_svc.run_action(first["id"], "标记生效")
        second = self._rule("50")
        self.quality_svc.run_action(second["id"], "标记生效")
        self.assertEqual(
            first["_last_check"]["report_code"],
            second["_last_check"]["report_code"],
        )
        self.assertEqual(len(store.rows("watermark_offset_reports")), 1)

    def test_activation_reads_current_business_version(self) -> None:
        """已有当前报告时质量核对直接读它，不另起检查版本。"""
        report, _ = self.watermark_svc.generate_offset_report(TOPIC, check_version="v-current")
        entry = self._rule("100")
        self.quality_svc.run_action(entry["id"], "标记生效")
        self.assertEqual(entry["_last_check"]["report_code"], report["报告编号"])
        self.assertEqual(entry["_last_check"]["check_version"], "v-current")
        self.assertEqual(len(store.rows("watermark_offset_reports")), 1)

    def test_repeat_check_does_not_duplicate_partition_alerts(self) -> None:
        """同一规则对同一版本重跑覆盖旧告警，不堆积重复分区告警。"""
        entry = self._rule("100")
        self.quality_svc.run_action(entry["id"], "标记生效")
        self.quality_svc.run_action(entry["id"], "提交校验")
        self.quality_svc.run_action(entry["id"], "标记生效")
        alerts = self.quality_svc.list_alerts(TOPIC)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0]["分区编号"], "2")

    def test_incomplete_report_raises_alert(self) -> None:
        """报告不完整时质量检查不能静默通过。"""
        report, _ = self.watermark_svc.generate_offset_report(TOPIC, check_version="v-partial", interrupt_after=1)
        self.assertEqual(report["状态"], "不完整")
        entry = self._rule("1000")
        watermark = next(row for row in store.rows("watermark_track") if row.get("数据主题") == TOPIC)
        check = self.quality_svc._run_quality_check(entry, watermark, report)
        self.assertFalse(check["passed"])
        self.assertTrue(any(item["type"] == "偏移报告不完整" for item in check["violations"]))

    def test_report_check_endpoint_payload(self) -> None:
        """按检查版本对账：报告、检查结果与分区告警同批返回。"""
        entry = self._rule("100")
        self.quality_svc.run_action(entry["id"], "标记生效")
        version = entry["_last_check"]["check_version"]
        payload = self.quality_svc.get_report_check(TOPIC, version)
        self.assertIsNotNone(payload)
        self.assertEqual(payload["来源批次"], "BATCH-20261116-1005")
        self.assertEqual(payload["偏移来源"], payload["报告"]["偏移来源"])
        self.assertEqual({alert["分区编号"] for alert in payload["分区告警"]}, {"2"})
        self.assertEqual(payload["检查结果"]["report_code"], payload["报告"]["报告编号"])


if __name__ == "__main__":
    unittest.main()
