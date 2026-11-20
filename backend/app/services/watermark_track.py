"""水位追踪业务规则：状态流转、字段校验与筛选口径都收在这里。

双水位偏移报告也在这里生成与落库：一个检查版本只产出一份报告，
偏移来源（事件时间 / 处理时间）与受影响分区随报告一起保存，供流式质量核对。
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from app.store import store

MODULE = "watermark_track"
LIST_FIELDS = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
REQUIRED_FIELDS = ["标记编号", "数据主题", "分区编号"]
STATUS_ORDER = ["草稿", "正常", "滞后", "已停用"]
ACTION_RULES = {"标记正常": "正常", "标记滞后": "滞后", "停用追踪": "已停用"}
NEGATIVE_ACTIONS = ["停用追踪"]

# 双水位偏移报告
REPORT_MODULE = "watermark_offset_reports"
SAMPLE_MODULE = "watermark_partition_samples"
EVENT_SOURCE = "事件时间"
PROCESS_SOURCE = "处理时间"
SOURCE_ALIASES = {
    "事件时间": EVENT_SOURCE, "event-time": EVENT_SOURCE, "event_time": EVENT_SOURCE, "event": EVENT_SOURCE,
    "处理时间": PROCESS_SOURCE, "processing-time": PROCESS_SOURCE, "processing_time": PROCESS_SOURCE, "processing": PROCESS_SOURCE,
}
DEFAULT_OFFSET_THRESHOLD = 300


def _parse_time(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except ValueError:
        return None


def _seconds(start: datetime | None, end: datetime | None) -> int | None:
    if start is None or end is None:
        return None
    return int((end - start).total_seconds())


class WatermarkTrackService:
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
            rows = [row for row in rows if keyword.casefold() in str(row.get("标记编号", "")).casefold()]
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
            return None, f"水位标记 {entry_id} 不存在或已归档"
        if action not in ACTION_RULES:
            return None, f"动作「{action}」不属于水位追踪可执行范围"
        target = ACTION_RULES[action]
        if target not in STATUS_ORDER:
            return None, f"目标状态「{target}」不在允许的状态序列里"
        entry["status"] = target
        entry["pending"] = target != STATUS_ORDER[-1]
        entry["abnormal"] = action in NEGATIVE_ACTIONS
        return entry, f"水位标记已{action}"

    # ───────────────────────────────
    # 双水位偏移报告
    # ───────────────────────────────

    def list_reports(
        self,
        *,
        topic: str | None = None,
        check_version: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = [row for row in store.rows(REPORT_MODULE)]
        if topic:
            rows = [row for row in rows if topic in str(row.get("数据主题", ""))]
        if check_version:
            rows = [row for row in rows if row.get("检查版本") == check_version]
        if status:
            rows = [row for row in rows if row.get("状态") == status]
        rows = sorted(rows, key=lambda row: (-int(row.get("版本序号", 0)), str(row.get("报告编号", ""))))
        total = len(rows)
        start = (page - 1) * size
        return [self._report_payload(row) for row in rows[start:start + size]], total

    def get_report(self, report_id: int) -> dict[str, Any] | None:
        return self._report_payload(store.find(REPORT_MODULE, report_id))

    def get_current_report(self, topic_code: str) -> dict[str, Any] | None:
        """当前业务版本：同一主题下版本序号最大且完整的那一份报告。"""
        candidates = [
            row for row in store.rows(REPORT_MODULE)
            if row.get("数据主题") == topic_code and row.get("状态") == "完整"
        ]
        if not candidates:
            return None
        return self._report_payload(max(candidates, key=lambda row: int(row.get("版本序号", 0))))

    def get_report_by_version(self, topic_code: str, check_version: str) -> dict[str, Any] | None:
        """按检查版本取报告；质量告警靠它绑定到同一次水位检查。"""
        return self._report_payload(self._find_report(topic_code, check_version))

    def report_overview(self) -> dict[str, Any]:
        """按当前业务版本给出水位偏移报告概览，列表与详情读同一份报告。"""
        reports = sorted(
            store.rows(REPORT_MODULE),
            key=lambda row: (str(row.get("数据主题", "")), -int(row.get("版本序号", 0))),
        )
        topics = sorted({str(row.get("数据主题", "")) for row in reports})
        views: list[dict[str, Any]] = []
        for topic in topics:
            topic_rows = [row for row in reports if row.get("数据主题") == topic]
            complete = [row for row in topic_rows if row.get("状态") == "完整"]
            current = max(complete, key=lambda row: int(row.get("版本序号", 0))) if complete else None
            views.append({
                "数据主题": topic,
                "主题名称": self._topic_name(topic),
                "当前检查版本": current.get("检查版本") if current else None,
                "当前报告编号": current.get("报告编号") if current else None,
                "当前偏移来源": current.get("偏移来源") if current else None,
                "当前受影响分区": list(current.get("受影响分区", [])) if current else [],
                "当前来源批次": current.get("来源批次") if current else None,
                "版本数": len(topic_rows),
                "完整版本数": len(complete),
                "未完成版本数": len(topic_rows) - len(complete),
                "当前报告": self._report_payload(current) if current else None,
            })

        versions = [
            {
                "报告编号": row.get("报告编号"),
                "数据主题": row.get("数据主题"),
                "检查版本": row.get("检查版本"),
                "偏移来源": row.get("偏移来源"),
                "受影响分区": list(row.get("受影响分区", [])),
                "来源批次": row.get("来源批次"),
                "状态": row.get("状态"),
            }
            for row in reports
        ]
        return {
            "cards": [
                {"label": "偏移报告数", "value": len(reports)},
                {"label": "已确认版本", "value": sum(1 for row in reports if row.get("状态") == "完整")},
                {"label": "受影响分区", "value": sum(len(row.get("受影响分区", [])) for row in reports if row.get("状态") == "完整")},
                {"label": "未完成报告", "value": sum(1 for row in reports if row.get("状态") != "完整")},
            ],
            "views": views,
            "versions": versions,
        }

    def generate_offset_report(
        self,
        topic_code: str,
        *,
        check_version: str | None = None,
        source: str | None = None,
        scope: str = "缺失",
        interrupt_after: int | None = None,
    ) -> tuple[dict[str, Any] | None, str]:
        """生成或补齐双水位偏移报告。

        - 同一「主题 + 检查版本」只产出一份报告，重跑返回原报告且偏移来源不变。
        - 检查中断时保留上一份完整报告，本版本标记为不完整并记住缺失分区。
        - 去重边界：已确认偏移的分区不会被重算覆盖；scope=全部 会覆盖已确认偏移，
          因此改为另起一个检查版本，原报告原样保留。
        """
        samples = self._partition_samples(topic_code)
        if not samples:
            return None, f"主题 {topic_code} 没有可比较的双水位分区记录"

        requested_source = SOURCE_ALIASES.get(str(source).strip()) if source else None
        if source and not requested_source:
            return None, f"偏移来源「{source}」无法识别，只能是事件时间或处理时间"

        resolved_version = check_version or self._default_version(topic_code)
        existing = self._find_report(topic_code, resolved_version)

        if existing is not None and existing.get("状态") == "完整":
            if scope != "全部":
                return self._report_payload(existing), (
                    f"检查版本 {resolved_version} 已有完整报告 {existing.get('报告编号')}，未重复生成")
            # 重算全部分区会覆盖已确认偏移，改走新版本，旧报告保持已确认。
            next_seq = self._next_version_seq(topic_code)
            resolved_version = f"{resolved_version}-R{next_seq}"
            replaced = existing
            existing = None
        else:
            replaced = None

        if existing is not None:
            # 不完整报告只补缺失分区，已确认分区不重算。
            return self._fill_missing(existing, samples, requested_source)

        return self._create_report(
            topic_code, samples, resolved_version, requested_source,
            interrupt_after=interrupt_after, replaced=replaced,
        )

    def retry_offset_report(self, report_id: int) -> tuple[dict[str, Any] | None, str]:
        """处理中断后重试：沿用同一检查版本，只补齐缺失分区。"""
        report = store.find(REPORT_MODULE, report_id)
        if report is None:
            return None, f"水位偏移报告 {report_id} 不存在或已归档"
        if report.get("状态") == "完整":
            return self._report_payload(report), f"报告 {report.get('报告编号')} 已完整，无需补齐"
        samples = self._partition_samples(str(report.get("数据主题", "")))
        if not samples:
            return None, f"主题 {report.get('数据主题')} 没有可比较的双水位分区记录"
        return self._fill_missing(report, samples, None)

    # ── 报告内部实现 ──

    def _partition_samples(self, topic_code: str) -> list[dict[str, Any]]:
        rows = [row for row in store.rows(SAMPLE_MODULE) if row.get("数据主题") == topic_code]
        return sorted(rows, key=lambda row: self._partition_key(row.get("分区编号")))

    @staticmethod
    def _partition_key(value: Any) -> tuple[int, str]:
        text = str(value)
        return (int(text), text) if text.isdigit() else (10 ** 9, text)

    def _default_version(self, topic_code: str) -> str:
        """默认检查版本取来源批次：同一窗口内重跑落在同一版本，不会反复出新报告。"""
        samples = self._partition_samples(topic_code)
        batch = str(samples[0].get("来源批次", "")).strip() if samples else ""
        return batch or f"v{self._next_version_seq(topic_code)}"

    def _next_version_seq(self, topic_code: str) -> int:
        seqs = [
            int(row.get("版本序号", 0)) for row in store.rows(REPORT_MODULE)
            if row.get("数据主题") == topic_code
        ]
        return max(seqs, default=0) + 1

    def _find_report(self, topic_code: str, check_version: str) -> dict[str, Any] | None:
        for row in store.rows(REPORT_MODULE):
            if row.get("数据主题") == topic_code and row.get("检查版本") == check_version:
                return row
        return None

    def _latest_complete(self, topic_code: str, exclude_id: int | None = None) -> dict[str, Any] | None:
        rows = [
            row for row in store.rows(REPORT_MODULE)
            if row.get("数据主题") == topic_code and row.get("状态") == "完整" and row.get("id") != exclude_id
        ]
        if not rows:
            return None
        return max(rows, key=lambda row: int(row.get("版本序号", 0)))

    def _create_report(
        self,
        topic_code: str,
        samples: list[dict[str, Any]],
        check_version: str,
        requested_source: str | None,
        *,
        interrupt_after: int | None,
        replaced: dict[str, Any] | None,
    ) -> tuple[dict[str, Any], str]:
        rows = store.rows(REPORT_MODULE)
        report_id = max((int(row.get("id", 0)) for row in rows), default=0) + 1
        covered = samples
        interrupted = interrupt_after is not None and interrupt_after < len(samples)
        if interrupted:
            covered = samples[:max(0, interrupt_after)]
        missing = [str(row.get("分区编号")) for row in samples if row not in covered]

        seq = self._next_version_seq(topic_code)
        previous = self._latest_complete(topic_code)

        report = {
            "id": report_id,
            "报告编号": f"WREP-{report_id:04d}",
            "数据主题": topic_code,
            "检查版本": check_version,
            "版本序号": seq,
            "检查窗口": str(samples[0].get("检查窗口", "")),
            "来源批次": str(samples[0].get("来源批次", "")),
            "状态": "不完整" if interrupted else "完整",
            "缺失分区": missing,
            "保留报告": previous.get("报告编号") if previous else None,
            "替代报告": replaced.get("报告编号") if replaced else None,
            "检查时间": datetime.now().isoformat(),
        }
        report.update(self._evaluate(samples, covered, requested_source))
        # 不完整版本不能声称偏移已确认，只保留已覆盖分区的对照。
        report["已确认偏移"] = not interrupted
        report["补齐"] = False
        report["补齐分区"] = []
        report["pending"] = interrupted
        report["abnormal"] = bool(report["受影响分区"]) or interrupted
        rows.append(report)
        return self._report_payload(report), (
            f"检查中断，报告 {report['报告编号']} 保留 {report['检查版本']} 已覆盖分区，缺失 {len(missing)} 个分区"
            if interrupted else f"水位偏移报告 {report['报告编号']} 已生成，偏移来源 {report['偏移来源']}"
        )

    def _fill_missing(
        self,
        report: dict[str, Any],
        samples: list[dict[str, Any]],
        requested_source: str | None,
    ) -> tuple[dict[str, Any], str]:
        """补齐缺失分区；已确认偏移的分区保持原样，不重算。

        中断期的偏移来源是只看部分分区得出的临时结论，补齐后按全部分区重新定源；
        调用方在中途指定改判来源则拒绝，同一检查版本只保留一个来源。
        """
        confirmed = [str(item.get("分区编号")) for item in report.get("分区明细", [])]
        missing_samples = [row for row in samples if str(row.get("分区编号")) not in confirmed]
        provisional_source = report.get("偏移来源")
        if requested_source and provisional_source and requested_source != provisional_source:
            return self._report_payload(report), (
                f"检查版本 {report.get('检查版本')} 中断期已按{provisional_source}记录偏移，"
                f"重试不能改判为{requested_source}，报告保持单一版本")

        if not missing_samples:
            return self._report_payload(report), f"报告 {report.get('报告编号')} 没有缺失分区"

        covered = list(samples)
        # 只把缺失分区并入明细，原有分区明细不覆盖。
        merged = {str(item.get("分区编号")): item for item in report.get("分区明细", [])}
        for item in self._partition_details(missing_samples):
            merged.setdefault(str(item.get("分区编号")), item)
        details = [merged[key] for key in sorted(merged, key=self._partition_key)]

        evaluation = self._evaluate(samples, covered, requested_source)
        evaluation["分区明细"] = details
        report.update(evaluation)
        # 来源只在补齐后一次定下来，中断期的临时来源留痕。
        report["中断期偏移来源"] = provisional_source
        if provisional_source and provisional_source != report.get("偏移来源"):
            report["来源判定"] = (
                f"{report.get('来源判定')}中断期只覆盖部分分区时曾按{provisional_source}记录，"
                f"补齐后按全部分区改判为{report.get('偏移来源')}。")
        report["状态"] = "完整"
        report["缺失分区"] = []
        report["已确认偏移"] = True
        report["补齐"] = True
        report["补齐时间"] = datetime.now().isoformat()
        report["补齐分区"] = [str(row.get("分区编号")) for row in missing_samples]
        report["pending"] = False
        if not report.get("保留报告"):
            previous = self._latest_complete(str(report.get("数据主题")), exclude_id=int(report.get("id", 0)))
            report["保留报告"] = previous.get("报告编号") if previous else None
        report["abnormal"] = bool(report["受影响分区"])
        return self._report_payload(report), (
            f"报告 {report.get('报告编号')} 已补齐 {len(missing_samples)} 个缺失分区，"
            f"偏移来源按全部分区确定为{report.get('偏移来源')}")

    def _evaluate(
        self,
        samples: list[dict[str, Any]],
        covered: list[dict[str, Any]],
        requested_source: str | None,
    ) -> dict[str, Any]:
        details = self._partition_details(covered)
        comparison = []
        stats: dict[str, dict[str, Any]] = {}
        for candidate in (EVENT_SOURCE, PROCESS_SOURCE):
            affected = [
                item["分区编号"] for item in details
                if item.get(f"{candidate}来源是否受影响")
            ]
            severity = sum(int(item.get(f"{candidate}滞后秒", 0)) for item in details)
            stats[candidate] = {"affected": affected, "severity": severity}
            comparison.append({
                "来源": candidate,
                "受影响分区": affected,
                "滞后合计秒": severity,
                "说明": f"若按{candidate}确定偏移，受影响分区为 {', '.join(affected) or '无'}",
            })

        if requested_source:
            chosen = requested_source
            decision_note = f"按调用方指定的{chosen}确定偏移。"
        else:
            # 受影响分区多者优先；同数量时看滞后合计，仍相同则固定取事件时间，保证重跑稳定。
            chosen = max(
                (EVENT_SOURCE, PROCESS_SOURCE),
                key=lambda name: (len(stats[name]["affected"]), stats[name]["severity"]),
            )
            differs = len(stats[EVENT_SOURCE]["affected"]) != len(stats[PROCESS_SOURCE]["affected"]) or \
                stats[EVENT_SOURCE]["severity"] != stats[PROCESS_SOURCE]["severity"]
            if not stats[EVENT_SOURCE]["affected"] and not stats[PROCESS_SOURCE]["affected"]:
                decision_note = f"已覆盖分区内两条水位都没有滞后，按{chosen}记录偏移来源。"
            elif differs:
                decision_note = f"两条水位指向不同滞后分区，报告只能给出一个检查版本，取受影响分区更多的{chosen}。"
            else:
                decision_note = f"两条水位指向同一批滞后分区，按{chosen}确定偏移。"

        reversed_partitions = self._opposite_partitions(details)
        movement = "；".join(
            f"{candidate}水位{self._movement_text(details, candidate)}"
            for candidate in (EVENT_SOURCE, PROCESS_SOURCE)
        )
        if reversed_partitions:
            movement = (
                f"分区 {', '.join(reversed_partitions)} 在同一检查窗口内两条水位反向移动（{movement}）")
        note = f"{movement}；报告按{chosen}确定偏移。{decision_note}"

        return {
            "偏移来源": chosen,
            "来源判定": note,
            "受影响分区": stats[chosen]["affected"],
            "事件时间滞后分区": stats[EVENT_SOURCE]["affected"],
            "处理时间滞后分区": stats[PROCESS_SOURCE]["affected"],
            "来源对照": comparison,
            "分区明细": details,
            "覆盖分区": [str(row.get("分区编号")) for row in samples if row in covered],
        }

    def _partition_details(self, samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
        details = []
        for sample in samples:
            event_prev = _parse_time(sample.get("上一事件时间水位"))
            event_now = _parse_time(sample.get("事件时间水位"))
            process_prev = _parse_time(sample.get("上一处理时间水位"))
            process_now = _parse_time(sample.get("处理时间水位"))
            event_delta = _seconds(event_prev, event_now)
            process_delta = _seconds(process_prev, process_now)
            details.append({
                "分区编号": str(sample.get("分区编号")),
                "上一事件时间水位": sample.get("上一事件时间水位"),
                "事件时间水位": sample.get("事件时间水位"),
                "事件时间偏移秒": event_delta,
                "事件时间滞后秒": self._lag(event_delta),
                "事件时间来源是否受影响": self._affected(event_delta),
                "上一处理时间水位": sample.get("上一处理时间水位"),
                "处理时间水位": sample.get("处理时间水位"),
                "处理时间偏移秒": process_delta,
                "处理时间滞后秒": self._lag(process_delta),
                "处理时间来源是否受影响": self._affected(process_delta),
                "延迟记录数": sample.get("延迟记录数"),
                "消费偏移": sample.get("消费偏移"),
                "来源批次": sample.get("来源批次"),
            })
        return details

    @staticmethod
    def _lag(delta: int | None) -> int:
        if delta is None:
            return 1
        return max(0, -delta) + (1 if delta == 0 else 0)

    @staticmethod
    def _affected(delta: int | None) -> bool:
        return delta is None or delta <= 0

    @staticmethod
    def _opposite_partitions(details: list[dict[str, Any]]) -> list[str]:
        """同一分区内事件时间与处理时间水位方向相反，才算反向移动。"""
        result = []
        for item in details:
            event_delta = item.get("事件时间偏移秒")
            process_delta = item.get("处理时间偏移秒")
            if event_delta is None or process_delta is None:
                continue
            if (event_delta < 0 < process_delta) or (process_delta < 0 < event_delta):
                result.append(str(item.get("分区编号")))
        return result

    @staticmethod
    def _movement_text(details: list[dict[str, Any]], candidate: str) -> str:
        deltas = [item.get(f"{candidate}偏移秒") for item in details if item.get(f"{candidate}偏移秒") is not None]
        if not deltas:
            return "缺少可比对记录"
        if min(deltas) < 0:
            return f"回退 {abs(min(deltas))} 秒"
        if max(deltas) == 0:
            return "停滞 0 秒"
        return f"前进 {max(deltas)} 秒"

    def _report_payload(self, row: dict[str, Any] | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = dict(row)
        payload["受影响分区"] = list(row.get("受影响分区", []))
        payload["缺失分区"] = list(row.get("缺失分区", []))
        payload["覆盖分区"] = list(row.get("覆盖分区", []))
        return payload

    def _topic_name(self, topic_code: str) -> str:
        for row in store.rows("topic_registry"):
            if row.get("主题编号") == topic_code:
                return str(row.get("主题名称", ""))
        return ""
