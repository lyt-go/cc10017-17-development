"""水位追踪接口：维护水位标记，覆盖标记正常、标记滞后、停用追踪等动作。"""
from __future__ import annotations

import csv
import io

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from app.schemas import ActionPayload, ActionResult, EntryPayload, PageResult
from app.services.watermark_track import WatermarkTrackService

router = APIRouter(prefix="/api/watermark_track", tags=["水位追踪"])

service = WatermarkTrackService()

LIST_FIELDS = ["标记编号", "数据主题", "分区编号", "事件时间水位", "处理时间水位", "延迟记录数", "消费偏移", "检查时间"]
STATUSES = ["草稿", "正常", "滞后", "已停用"]


def query_filters(request: Request) -> dict[str, str]:
    """只接受当前模块声明过的字段，忽略分页等保留参数。"""
    return {
        key: value.strip()
        for key, value in request.query_params.items()
        if key in LIST_FIELDS and value.strip()
    }


@router.get("", response_model=PageResult[dict])
def list_entries(
    request: Request,
    keyword: str | None = Query(default=None, description="按标记编号检索"),
    status: str | None = Query(default=None, description="草稿、正常、滞后、已停用"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按页面字段、标记编号关键字与状态过滤水位追踪列表。"""
    items, total = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/export")
def export_entries(
    request: Request,
    keyword: str | None = Query(default=None),
    status: str | None = Query(default=None),
) -> StreamingResponse:
    """下载当前筛选条件下的 UTF-8 CSV；必须声明在 /{entry_id} 之前。"""
    items, _ = service.list_entries(
        filters=query_filters(request), keyword=keyword, status=status, page=1, size=1000000)
    output = io.StringIO()
    output.write("\ufeff")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(["id", *LIST_FIELDS, "status"])
    for item in items:
        row = []
        for value in [item.get("id"), *(item.get(field, "") for field in LIST_FIELDS), item.get("status", "")]:
            text = "" if value is None else str(value)
            row.append("'" + text if text.startswith(("=", "+", "-", "@")) else text)
        writer.writerow(row)
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": 'attachment; filename="watermark_track.csv"'})


@router.get("/reports/overview")
def report_overview() -> dict:
    """按当前业务版本给出双水位偏移报告概览，列表与详情读同一份报告。"""
    return service.report_overview()


@router.get("/reports", response_model=PageResult[dict])
def list_reports(
    topic: str | None = Query(default=None, description="数据主题"),
    check_version: str | None = Query(default=None, description="检查版本"),
    status: str | None = Query(default=None, description="完整、不完整"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=20, ge=1, le=200),
) -> PageResult[dict]:
    """按主题、检查版本与状态过滤水位偏移报告。"""
    items, total = service.list_reports(
        topic=topic, check_version=check_version, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/reports/{report_id}", response_model=dict)
def get_report(report_id: int) -> dict:
    """读取一份水位偏移报告，保留偏移来源与受影响分区。"""
    report = service.get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail=f"水位偏移报告 {report_id} 不存在或已归档")
    return report


@router.post("/reports/generate", response_model=ActionResult)
def generate_report(payload: dict[str, object]) -> ActionResult:
    """生成双水位偏移报告；scope=全部 表示重算全部分区。"""
    topic_code = str(payload.get("topic") or payload.get("数据主题") or "").strip()
    if not topic_code:
        return ActionResult(ok=False, message="缺少数据主题")
    report, message = service.generate_offset_report(
        topic_code,
        check_version=(str(payload.get("check_version")).strip() if payload.get("check_version") else None),
        source=(str(payload.get("source")).strip() if payload.get("source") else None),
        scope=str(payload.get("scope") or "缺失"),
        interrupt_after=(int(payload["interrupt_after"]) if payload.get("interrupt_after") is not None else None),
    )
    return ActionResult(ok=report is not None, message=message, entry=report)


@router.post("/reports/{report_id}/retry", response_model=ActionResult)
def retry_report(report_id: int) -> ActionResult:
    """处理中断后重试：沿用同一检查版本，只补齐缺失分区。"""
    report, message = service.retry_offset_report(report_id)
    return ActionResult(ok=report is not None, message=message, entry=report)


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条水位标记明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"水位标记 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: dict[str, object]) -> ActionResult:
    """登记一条水位标记，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload)
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="水位标记已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: ActionPayload) -> ActionResult:
    """对单条水位标记执行标记正常、标记滞后、停用追踪；不允许的动作会被拦下并说明原因。"""
    entry, message = service.run_action(entry_id, payload.action.strip())
    if entry is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=entry)
