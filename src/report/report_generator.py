import os
import textwrap
from typing import Dict, Iterable, List, Optional

import pandas as pd
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen import canvas


PAGE_W, PAGE_H = A4
LEFT = 2 * cm
RIGHT = PAGE_W - 2 * cm
TEXT_WIDTH = RIGHT - LEFT


def _fmt_pct(x) -> str:
    try:
        return f"{float(x) * 100:.1f}%"
    except Exception:
        return "NA"


def _fmt_money(x) -> str:
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "NA"


def _fmt_decimal(x, digits: int = 4) -> str:
    try:
        return f"{float(x):.{digits}f}"
    except Exception:
        return "NA"


def _as_bool_series(series: pd.Series) -> pd.Series:
    return series.astype(str).str.lower().eq("true")


def _draw_wrapped_lines(c: canvas.Canvas, lines: Iterable[str], x: float, y: float, width_chars: int = 105, leading: float = 12) -> float:
    text = c.beginText(x, y)
    text.setLeading(leading)
    for line in lines:
        if not line:
            text.textLine("")
            continue
        for wrapped in textwrap.wrap(line, width=width_chars):
            text.textLine(wrapped)
    c.drawText(text)
    return y - leading * sum(max(1, len(textwrap.wrap(line, width=width_chars))) if line else 1 for line in lines)


def _latest_full_month(kpi: pd.DataFrame) -> Dict:
    kpi = kpi.copy()
    kpi["month"] = kpi["month"].astype(str)
    kpi["is_partial_month"] = _as_bool_series(kpi["is_partial_month"])
    full = kpi.loc[~kpi["is_partial_month"]].sort_values("month")
    return full.iloc[-1].to_dict()


def _latest_stable_retention(customer_health: pd.DataFrame, kpi: pd.DataFrame) -> Dict:
    customer_health = customer_health.copy()
    customer_health["month"] = customer_health["month"].astype(str)

    kpi = kpi.copy()
    kpi["month"] = kpi["month"].astype(str)
    kpi["is_partial_month"] = _as_bool_series(kpi["is_partial_month"])
    months = kpi.sort_values("month")["month"].tolist()
    month_to_partial = dict(zip(kpi["month"], kpi["is_partial_month"]))

    stable_months: List[str] = []
    for idx, month in enumerate(months[:-1]):
        next_month = months[idx + 1]
        if not month_to_partial.get(next_month, True):
            stable_months.append(month)

    stable = customer_health[customer_health["month"].isin(stable_months)].sort_values("month")
    if stable.empty:
        return {}
    return stable.iloc[-1].to_dict()


def _section_header(c: canvas.Canvas, title: str, subtitle: Optional[str] = None) -> float:
    c.setFont("Helvetica-Bold", 15)
    c.drawString(LEFT, PAGE_H - 2.0 * cm, title)
    y = PAGE_H - 2.9 * cm
    if subtitle:
        c.setFont("Helvetica", 9)
        y = _draw_wrapped_lines(c, [subtitle], LEFT, y, width_chars=104, leading=11)
        y -= 0.4 * cm
    return y


def _draw_image_if_exists(c: canvas.Canvas, title: str, path: str, x: float, y_top: float, width: float, height: float) -> float:
    c.setFont("Helvetica-Bold", 10)
    c.drawString(x, y_top, title)
    y_top -= 0.35 * cm
    if os.path.exists(path):
        c.drawImage(path, x, y_top - height, width=width, height=height, preserveAspectRatio=True, anchor="sw")
    else:
        c.setFont("Helvetica", 9)
        c.drawString(x, y_top - 0.6 * cm, f"(Missing figure: {os.path.basename(path)})")
    return y_top - height - 0.5 * cm


def generate_business_report(
    kpi_csv: str,
    customer_health_csv: str,
    top_return_skus_csv: str,
    figures_dir: str,
    out_pdf: str,
) -> None:
    """Generate a 3-page business summary aligned to the dashboard's main analysis."""

    out_dir = os.path.dirname(out_pdf)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    kpi = pd.read_csv(kpi_csv)
    customer_health = pd.read_csv(customer_health_csv)
    top_return_skus = pd.read_csv(top_return_skus_csv)

    kpi["month"] = kpi["month"].astype(str)
    customer_health["month"] = customer_health["month"].astype(str)
    top_return_skus["month"] = top_return_skus["month"].astype(str)
    kpi["is_partial_month"] = _as_bool_series(kpi["is_partial_month"])

    latest_full = _latest_full_month(kpi)
    latest_full_month = latest_full.get("month", "NA")
    latest_customer = customer_health[customer_health["month"] == latest_full_month].sort_values("month")
    latest_customer = latest_customer.iloc[-1].to_dict() if not latest_customer.empty else {}
    latest_retention = _latest_stable_retention(customer_health, kpi)

    partial_note = None
    partial_rows = kpi[kpi["is_partial_month"]].sort_values("month")
    if not partial_rows.empty:
        partial_month = partial_rows.iloc[-1].to_dict().get("month", "NA")
        partial_note = (
            f"Partial-month handling: {partial_month} is incomplete in the source data and is shown as a watchout signal only, not as a normal comparison point for confirmed conclusions."
        )

    latest_top_returns = (
        top_return_skus[top_return_skus["month"] == latest_full_month]
        .sort_values(["return_burden", "rank"], ascending=[False, True])
        .head(5)
    )

    c = canvas.Canvas(out_pdf, pagesize=A4)

    # Page 1 - Executive Summary
    y = _section_header(
        c,
        "BUSINESS REPORT - Monthly Ecommerce Performance Monitoring",
        "This report summarizes the project's primary monitoring views. Supporting analyses such as MoM variance and cohort output remain in Python outputs and documentation rather than the main report.",
    )
    c.setFont("Helvetica-Bold", 12)
    c.drawString(LEFT, y, "1) Executive Summary")
    y -= 0.6 * cm

    c.setFont("Helvetica", 10)
    lines = [
        f"Latest full-month snapshot ({latest_full_month}):",
        f"- Product Sales (before returns): {_fmt_money(latest_full.get('product_sales_before_returns', latest_full.get('net_gmv')))}",
        f"- Return Burden: {_fmt_pct(latest_full.get('return_rate'))}",
        f"- Top50 SKU Coverage: {_fmt_pct(latest_full.get('top50_coverage'))}",
        f"- SKU HHI: {_fmt_decimal(latest_full.get('hhi'), 4)}",
        "",
        "Business reading:",
        "- Product sales are monitored across the monthly series, while the latest full month is used as the formal current-state summary point.",
        "- Return burden remains a monitoring signal rather than direct proof of quality or logistics failure.",
        "- Product concentration is tracked using both Top50 SKU Coverage (head concentration) and SKU HHI (overall concentration structure).",
    ]
    if partial_note:
        lines.extend(["", partial_note])
    y = _draw_wrapped_lines(c, lines, LEFT, y, width_chars=108, leading=12)

    chart_top = y - 0.2 * cm
    left_x = LEFT
    right_x = LEFT + 8.8 * cm
    chart_w = 8.0 * cm
    chart_h = 5.2 * cm
    _draw_image_if_exists(
        c,
        "Product Sales Trend",
        os.path.join(figures_dir, "product_sales_before_returns_trend.png"),
        left_x,
        chart_top,
        chart_w,
        chart_h,
    )
    _draw_image_if_exists(
        c,
        "Return Burden Trend",
        os.path.join(figures_dir, "return_burden_trend.png"),
        right_x,
        chart_top,
        chart_w,
        chart_h,
    )
    c.showPage()

    # Page 2 - Customer Health
    y = _section_header(
        c,
        "2) Customer Health",
        "Customer-level views in this report apply to identified customers only. Coverage metrics are included to show how representative those views are relative to total product sales.",
    )

    c.setFont("Helvetica", 10)
    lines = [
        f"Latest full-month identified sales coverage ({latest_full_month}): {_fmt_pct(latest_customer.get('identified_sales_coverage'))}",
        f"Latest stable next-month retention ({latest_retention.get('month', 'NA')}): {_fmt_pct(latest_retention.get('next_month_repeat_rate'))}",
        f"Latest full-month Top10 Customer Share ({latest_full_month}): {_fmt_pct(latest_customer.get('top10_share'))}",
        f"Latest full-month Customer HHI ({latest_full_month}): {_fmt_decimal(latest_customer.get('customer_hhi'), 4)}",
        "",
        "Business reading:",
        "- Identified sales coverage shows how much of product sales can actually support customer-level analysis.",
        "- Retention is treated as a customer health signal, with the latest stable point excluding periods whose following month is incomplete.",
        "- Top10 Customer Share and Customer HHI are used together to assess whether identified-customer sales are overly dependent on a narrow customer base.",
    ]
    y = _draw_wrapped_lines(c, lines, LEFT, y, width_chars=108, leading=12)

    chart_top = y - 0.2 * cm
    _draw_image_if_exists(
        c,
        "Identified Sales Coverage Trend",
        os.path.join(figures_dir, "identified_sales_coverage_trend.png"),
        left_x,
        chart_top,
        chart_w,
        chart_h,
    )
    _draw_image_if_exists(
        c,
        "Next-Month Retention Trend",
        os.path.join(figures_dir, "next_month_retention_trend.png"),
        right_x,
        chart_top,
        chart_w,
        chart_h,
    )
    c.showPage()

    # Page 3 - Returns / Product Risk
    y = _section_header(
        c,
        "3) Returns / Product Risk",
        "Return analysis focuses on merchandise returns only. Top return SKU outputs are used for driver discovery and drill-down prioritization rather than direct root-cause proof.",
    )

    c.setFont("Helvetica", 10)
    lines = [
        f"Latest full-month return burden ({latest_full_month}): {_fmt_pct(latest_full.get('return_rate'))}",
        f"Latest full-month merchandise return value ({latest_full_month}): {_fmt_money(abs(float(latest_full.get('return_value', 0))))}",
        f"Latest full-month merchandise return units ({latest_full_month}): {_fmt_money(latest_full.get('return_unit'))}",
        "",
        "Latest full-month top return SKU watchlist:",
    ]
    for _, row in latest_top_returns.iterrows():
        lines.append(f"- {row.get('Description', 'NA')}: {_fmt_money(row.get('return_burden'))}")
    if latest_top_returns.empty:
        lines.append("- No latest full-month top return SKU output available.")
    lines.extend([
        "",
        "Business reading:",
        "- Aggregate return burden can remain relatively low while still warranting SKU-level drill-down.",
        "- Top return SKUs indicate which products deserve further review, but extreme values still require transaction-level validation before stronger attribution.",
    ])
    y = _draw_wrapped_lines(c, lines, LEFT, y, width_chars=108, leading=12)

    chart_top = y - 0.2 * cm
    _draw_image_if_exists(
        c,
        "Return Burden Trend",
        os.path.join(figures_dir, "return_burden_trend.png"),
        left_x,
        chart_top,
        chart_w,
        chart_h,
    )
    _draw_image_if_exists(
        c,
        "Top Return SKUs (Latest Full Month)",
        os.path.join(figures_dir, "top_return_skus_latest_full_month.png"),
        right_x,
        chart_top,
        chart_w,
        chart_h,
    )

    c.save()
