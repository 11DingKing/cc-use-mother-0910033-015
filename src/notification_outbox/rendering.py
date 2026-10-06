"""按渠道渲染不同内容的通知。

模板名对应接收方记录中的 ``template`` 字段；同一排班变更批准事件，
学校、讲解员、场馆分别使用不同模板与参数，得到不同内容。
"""
from __future__ import annotations

from collections.abc import Mapping


class TemplateError(KeyError):
    """模板缺失或占位变量不全。"""


# (主题, 正文)，正文使用 str.format 占位符。
TEMPLATES: dict[str, tuple[str, str]] = {
    "schedule_approved_school": (
        "排班已批准：{change_id}",
        "学校 {school_name} 您好：贵单位申请的 {visit_date} 参观排班（{change_id}）已批准，"
        "讲解员 {docent_name}，请按时到达 {venue_name}。",
    ),
    "schedule_approved_docent": (
        "新的讲解任务：{visit_date}",
        "{docent_name} 您好：{visit_date} {start_time} 请到 {venue_name} "
        "为 {school_name} 提供讲解，排班号 {change_id}，请确认。",
    ),
    "schedule_approved_venue": (
        "场馆接待安排：{visit_date}",
        "{venue_name}：{visit_date} {start_time} 接待 {school_name}，"
        "讲解员 {docent_name}，排班号 {change_id}，请预留场地。",
    ),
}


class TemplateRenderer:
    def __init__(self, templates: Mapping[str, tuple[str, str]] | None = None) -> None:
        self._templates = dict(templates if templates is not None else TEMPLATES)

    def render(self, template: str, params: Mapping[str, object]) -> tuple[str, str]:
        try:
            subject_tpl, body_tpl = self._templates[template]
        except KeyError:
            raise TemplateError(f"未知模板：{template}") from None
        try:
            return (
                subject_tpl.format(**params),
                body_tpl.format(**params),
            )
        except KeyError as exc:
            raise TemplateError(f"模板 {template} 缺少变量：{exc.args[0]}") from None
