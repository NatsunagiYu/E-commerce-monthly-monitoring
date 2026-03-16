import os
import sys
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.ingest.loader import load_csv
from src.transforms.clean import clean_data

from src.kpi.kpi_calculations import monthly_kpis
from src.variance.variance_analysis import mom_variance
from src.report.report_generator import generate_business_report

from src.customer.customer_analysis import (
    retention,
    customer_concentration,
    cohort_table,
    customer_id_coverage,
    top_return_skus
)


def ensure_dirs():
    os.makedirs("outputs/tables", exist_ok=True)
    os.makedirs("outputs/figures", exist_ok=True)
    os.makedirs("outputs/reports", exist_ok=True)


def save_table(df: pd.DataFrame, path: str):
    df.to_csv(path, index=False)
    print(f" Saved table: {path}")


def plot_line(df: pd.DataFrame, x_col: str, y_col: str, title: str, out_path: str):
    plt.figure(figsize=(7, 3.6))
    plt.plot(df[x_col].astype(str), df[y_col])
    plt.title(title)
    plt.xlabel(x_col)
    plt.ylabel(y_col)
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f" Saved figure: {out_path}")


def plot_top_return_skus_latest_full_month(top_returns: pd.DataFrame, kpi: pd.DataFrame, out_path: str):
    kpi = kpi.copy()
    kpi["month"] = kpi["month"].astype(str)
    kpi["is_partial_month"] = kpi["is_partial_month"].astype(str).str.lower().eq("true")
    full_months = kpi.loc[~kpi["is_partial_month"]].sort_values("month")
    latest_full_month = full_months.iloc[-1]["month"]

    data = top_returns[top_returns["month"].astype(str) == latest_full_month].copy()
    data = data.sort_values(["return_burden", "rank"], ascending=[True, False]).tail(5)

    plt.figure(figsize=(7, 3.6))
    plt.barh(data["Description"], data["return_burden"])
    plt.title(f"Top Return SKUs ({latest_full_month})")
    plt.xlabel("Return Burden")
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()
    print(f" Saved figure: {out_path}")


def build_customer_health_monthly(
    coverage: pd.DataFrame,
    repeat: pd.DataFrame,
    conc: pd.DataFrame
) -> pd.DataFrame:
    coverage = coverage.copy()
    repeat = repeat.copy()
    conc = conc.copy()

    coverage["month"] = coverage["month"].astype(str)
    repeat["month"] = repeat["month"].astype(str)
    conc["month"] = conc["month"].astype(str)

    merged = coverage.merge(repeat, on="month", how="left")
    merged = merged.merge(conc, on="month", how="left")
    merged = merged.sort_values("month").reset_index(drop=True)

    return merged


def build_dim_month(kpi: pd.DataFrame) -> pd.DataFrame:
    dim = kpi[["month", "is_partial_month"]].copy()
    dim["month"] = dim["month"].astype(str)
    dim["month_start"] = pd.to_datetime(dim["month"] + "-01")
    dim["year"] = dim["month_start"].dt.year
    dim["month_num"] = dim["month_start"].dt.month
    dim["year_month_label"] = dim["month_start"].dt.strftime("%Y-%m")
    dim = dim.sort_values("month_start").reset_index(drop=True)
    return dim


def main():
    ensure_dirs()

    data_path = "data/raw/online_retail.csv"
    print(f"Loading: {data_path}")
    df_raw = load_csv(data_path)

    print("Cleaning data...")
    df = clean_data(df_raw)

    print("Computing monthly KPIs...")
    kpi = monthly_kpis(df, month_col="month")
    save_table(kpi, "outputs/tables/kpi_monthly.csv")

    print("Adding MoM variance...")
    kpi_mom = mom_variance(kpi, month_col="month")
    save_table(kpi_mom, "outputs/tables/kpi_monthly_with_mom.csv")

    print("Computing customer ID coverage...")
    coverage = customer_id_coverage(df, month_col="month")
    save_table(coverage, "outputs/tables/customer_id_coverage.csv")

    print("Computing customer retention...")
    repeat = retention(df, month_col="month")
    save_table(repeat, "outputs/tables/customer_repeat_rate.csv")

    print("Computing customer concentration...")
    conc = customer_concentration(df, month_col="month", n=10)
    save_table(conc, "outputs/tables/customer_concentration.csv")

    print("Computing cohort table...")
    cohort = cohort_table(df, month_col="month", freq="M")
    save_table(cohort, "outputs/tables/cohort_table.csv")

    print("Computing top return SKUs...")
    top_returns = top_return_skus(df, month_col="month", top_n=10)
    save_table(top_returns, "outputs/tables/top_return_skus.csv")

    print("Building Power BI-ready customer health table...")
    customer_health = build_customer_health_monthly(coverage, repeat, conc)
    save_table(customer_health, "outputs/tables/customer_health_monthly.csv")

    print("Building Power BI month dimension...")
    dim_month = build_dim_month(kpi)
    save_table(dim_month, "outputs/tables/dim_month.csv")

    print("Generating figures...")
    plot_line(
        kpi,
        "month",
        "product_sales_before_returns",
        "Product Sales (before returns) Trend (Monthly)",
        "outputs/figures/product_sales_before_returns_trend.png"
    )
    plot_line(
        kpi,
        "month",
        "return_rate",
        "Return Burden Trend (Monthly)",
        "outputs/figures/return_burden_trend.png"
    )
    plot_line(
        kpi,
        "month",
        "top50_coverage",
        "Top50 Coverage Trend (Monthly)",
        "outputs/figures/top50_coverage_trend.png"
    )
    plot_line(
        kpi,
        "month",
        "hhi",
        "SKU HHI Trend (Monthly)",
        "outputs/figures/sku_hhi_trend.png"
    )
    plot_line(
        customer_health,
        "month",
        "identified_sales_coverage",
        "Identified Sales Coverage Trend (Monthly)",
        "outputs/figures/identified_sales_coverage_trend.png"
    )
    plot_line(
        repeat,
        "month",
        "next_month_repeat_rate",
        "Next-Month Retention Trend (Monthly)",
        "outputs/figures/next_month_retention_trend.png"
    )
    plot_top_return_skus_latest_full_month(
        top_returns,
        kpi,
        "outputs/figures/top_return_skus_latest_full_month.png"
    )

    print("Pipeline finished. Check outputs/ folder.")

    print("📄 Generating BUSINESS_REPORT.pdf...")
    generate_business_report(
        kpi_csv="outputs/tables/kpi_monthly.csv",
        customer_health_csv="outputs/tables/customer_health_monthly.csv",
        top_return_skus_csv="outputs/tables/top_return_skus.csv",
        figures_dir="outputs/figures",
        out_pdf="outputs/reports/BUSINESS_REPORT.pdf"
    )

    print(" Saved: outputs/reports/BUSINESS_REPORT.pdf")


if __name__ == "__main__":
    main()