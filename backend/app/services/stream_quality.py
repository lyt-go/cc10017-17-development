"""流式质量业务规则：质量规则管理、校验执行与死信联动。

增强：质量规则生效时触发水位检查，异常时生成死信记录。
质量告警绑定水位追踪生成的双水位偏移报告：按同一检查版本核对受影响分区，
告警里保留报告编号与来源批次，跨模块交接不丢批次。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.services.watermark_track import WatermarkTrackService
from app.store import store

MODULE = "stream_quality"
LIST_FIELDS = ["规则编号", "监控主题", "校验类型", "告警阈值", "采样比例", "检测窗口", "阻断动作", "规则版本"]
REQUIRED_FIELDS = ["规则编号", "监控主题", "校验类型"]
STATUS_ORDER = ["草稿", "待校验", "已生效", "已停用"]
ACTION_RULES = {"提交校验": "待校验", "标记生效": "已生效", "停用规则": "已停用"}
NEGATIVE_ACTIONS = ["停用规则"]

ALERT_MODULE = "quality_partition_alerts"


class StreamQualityService:
    def __init__(self) -> None:
        self._watermarks = WatermarkTrackService()

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = store.rows(MODULE)
        for field, expected in (filters or {}).items():
            rows = [row for row in rows if expected.casefold() in str(row.get(field, "")).casefold()]
        if keyword:
            rows = [row for row in rows if keyword.casefold() in str(row.get("规则编号", "")).casefold()]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        total = len(rows)
        start = (page - 1) * size
        return rows[start:start + size], total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return store.find(MODULE, entry_id)

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [field for field in REQUIRED_FIELDS if not str(values.get(field) or "").strip()]
        if missing:
            return None, missing
        # 校验监控主题
        topic_code = values.get("监控主题")
        if topic_code:
            topic = self._find_topic(topic_code)
            if not topic:
                return None, [f"监控主题 {topic_code} 未注册"]

        rows = store.rows(MODULE)
        entry = {"id": max((int(row.get("id", 0)) for row in rows), default=0) + 1}
        entry.update({field: values.get(field) for field in LIST_FIELDS})
        entry["status"] = STATUS_ORDER[0]
        entry["pending"] = True
        entry["abnormal"] = False
        rows.append(entry)
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = store.find(MODULE, entry_id)
        if entry is None:
            return None, f"质量规则 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于流式质量可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"

        if action == "标记生效":
            topic_code = entry.get("监控主题")
            watermark = self._find_watermark(topic_code)
            if not watermark:
                return None, f"主题 {topic_code} 未配置水位追踪"
            # 核对当前业务版本的水位偏移报告；还没有报告时才让水位追踪生成一份。
            report = self._watermarks.get_current_report(topic_code)
            if report is None:
                report, report_message = self._watermarks.generate_offset_report(topic_code)
                if report is None:
                    return None, f"主题 {topic_code} 的水位偏移报告未能生成：{report_message}"
            entry["_last_offset_report"] = report
            self._run_quality_check(entry, watermark, report)

        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"质量规则已{action}"

    def _find_topic(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return row
        return None

    def _find_watermark(self, topic_code: str) -> dict[str, Any] | None:
        for row in store.rows("watermark_track"):
            if row.get("数据主题") == topic_code:
                return row
        return None

    def _run_quality_check(
        self,
        entry: dict[str, Any],
        watermark: dict[str, Any],
        report: dict[str, Any],
    ) -> dict[str, Any]:
        """核对水位偏移报告并生成分区级告警，异常时产生死信记录。"""
        try:
            threshold = float(entry.get("告警阈值", "0"))
        except (ValueError, TypeError):
            threshold = 0.0

        details = {str(item.get("分区编号")): item for item in report.get("分区明细", [])}
        affected = [str(partition) for partition in report.get("受影响分区", [])]
        source = report.get("偏移来源")

        violations = []
        # 1) 规则阈值按报告受影响分区的延迟记录数核对。
        for partition in affected:
            detail = details.get(partition, {})
            try:
                late_count = float(detail.get("延迟记录数", "0"))
            except (ValueError, TypeError):
                late_count = 0.0
            if late_count > threshold:
                violations.append({
                    "type": "延迟超限",
                    "detail": f"分区 {partition} 延迟记录 {int(late_count)} 超过阈值 {threshold}",
                    "topic": entry.get("监控主题"),
                    "partition": partition,
                })
                # 产生死信
                self._create_dead_letter(entry, detail or watermark, "延迟超限")

        # 2) 报告未完成时，缺失分区本身就是一条质量告警，不能静默通过。
        if report.get("状态") != "完整":
            violations.append({
                "type": "偏移报告不完整",
                "detail": f"报告 {report.get('报告编号')} 缺少分区 {', '.join(report.get('缺失分区', []))}",
                "topic": entry.get("监控主题"),
                "partition": None,
            })

        # 3) 双水位偏移报告只能给出一个检查版本，但两条水位各自点名的滞后分区都要核对。
        for comparison in report.get("来源对照", []):
            if comparison.get("来源") == source:
                continue
            others = [p for p in comparison.get("受影响分区", []) if p not in affected]
            if others:
                violations.append({
                    "type": "偏移来源冲突",
                    "detail": f"报告按{source}确定偏移，{comparison.get('来源')}还指向分区 {', '.join(others)}",
                    "topic": entry.get("监控主题"),
                    "partition": None,
                    "conflict_partitions": others,
                })

        alerts = self._write_partition_alerts(entry, report, violations)

        try:
            late_count = float(watermark.get("延迟记录数", "0"))
        except (ValueError, TypeError):
            late_count = 0.0

        result = {
            "rule_id": entry.get("id"),
            "rule_code": entry.get("规则编号"),
            "topic": entry.get("监控主题"),
            "threshold": threshold,
            "late_count": late_count,
            "violations": violations,
            "report_id": report.get("id"),
            "report_code": report.get("报告编号"),
            "check_version": report.get("检查版本"),
            "batch": report.get("来源批次"),
            "offset_source": source,
            "affected_partitions": affected,
            "alerts": alerts,
            "checked_at": datetime.now().isoformat(),
            "passed": len(violations) == 0,
        }
        self._ensure_artifact_table("quality_check_results")
        store.rows("quality_check_results").append(result)
        entry["_last_check"] = result
        return result

    def _write_partition_alerts(
        self,
        entry: dict[str, Any],
        report: dict[str, Any],
        violations: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        """把核对结果落成按分区的质量告警，保留报告编号与来源批次。"""
        rows = store.rows(ALERT_MODULE)
        by_partition: dict[str, list[dict[str, Any]]] = {}
        for violation in violations:
            partition = violation.get("partition")
            if partition is None:
                continue
            by_partition.setdefault(str(partition), []).append(violation)

        alerts = []
        for partition, items in sorted(by_partition.items()):
            # 同一规则对同一检查版本重跑时覆盖旧告警，不堆积重复分区告警。
            same = next((
                row for row in rows
                if row.get("规则编号") == entry.get("规则编号")
                and row.get("报告编号") == report.get("报告编号")
                and row.get("分区编号") == partition
            ), None)
            if same is not None:
                same["告警说明"] = "；".join(str(item.get("detail", "")) for item in items)
                same["偏移来源"] = report.get("偏移来源")
                same["来源批次"] = report.get("来源批次")
                same["created_at"] = datetime.now().isoformat()
                alerts.append(same)
                continue
            alert = {
                "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
                "告警编号": f"QALT-{len(rows) + 1:04d}",
                "规则编号": entry.get("规则编号"),
                "监控主题": entry.get("监控主题"),
                "分区编号": partition,
                "偏移来源": report.get("偏移来源"),
                "报告编号": report.get("报告编号"),
                "检查版本": report.get("检查版本"),
                "来源批次": report.get("来源批次"),
                "告警类型": "延迟超限",
                "告警说明": "；".join(str(item.get("detail", "")) for item in items),
                "状态": "待处置",
                "pending": True,
                "abnormal": True,
                "检查时间": report.get("检查时间"),
                "created_at": datetime.now().isoformat(),
            }
            rows.append(alert)
            alerts.append(alert)
        return alerts

    def list_alerts(self, topic_code: str | None = None) -> list[dict[str, Any]]:
        rows = [row for row in store.rows(ALERT_MODULE)]
        if topic_code:
            rows = [row for row in rows if row.get("监控主题") == topic_code]
        return rows

    def get_report_check(self, topic_code: str, check_version: str) -> dict[str, Any] | None:
        """按下发的检查版本取同一次检查的质量结果，供水位页与质量页对账。"""
        reports = self._watermarks.get_report_by_version(topic_code, check_version)
        if reports is None:
            return None
        checks = [
            row for row in store.rows("quality_check_results")
            if row.get("topic") == topic_code and row.get("check_version") == check_version
        ]
        alerts = [
            row for row in store.rows(ALERT_MODULE)
            if row.get("监控主题") == topic_code and row.get("检查版本") == check_version
        ]
        return {
            "报告": reports,
            "检查结果": checks[-1] if checks else None,
            "分区告警": alerts,
            "偏移来源": reports.get("偏移来源"),
            "受影响分区": list(reports.get("受影响分区", [])),
            "来源批次": reports.get("来源批次"),
        }

    def _create_dead_letter(self, entry: dict[str, Any], watermark: dict[str, Any], reason: str) -> None:
        dl_rows = store.rows("dead_letter")
        dl_id = max((int(r.get("id", 0)) for r in dl_rows), default=0) + 1
        dl = {
            "id": dl_id,
            "status": "待处置",
            "pending": True,
            "abnormal": True,
            "记录编号": f"DEAD-{dl_id:04d}",
            "原始主题": entry.get("监控主题"),
            "原始分区": watermark.get("分区编号", "0"),
            "失败原因": reason,
            "原始消息体": f"延迟记录数:{watermark.get('延迟记录数')}",
            "重试次数": "0",
            "入库时间": datetime.now().isoformat(),
            "处置状态": "待分析",
        }
        dl_rows.append(dl)

    def get_quality_results(self, entry_id: int) -> list[dict[str, Any]]:
        self._ensure_artifact_table("quality_check_results")
        return [r for r in store.rows("quality_check_results") if r.get("rule_id") == entry_id]

    def _ensure_artifact_table(self, name: str) -> None:
        store.rows(name)
