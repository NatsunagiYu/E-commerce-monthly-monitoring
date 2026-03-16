import pandas as pd


def topn_coverage(df: pd.DataFrame, month_col="month", n: int = 50) -> pd.DataFrame:
    df = df.copy()
    df = df[df["type"] == "PRODUCT"]

    df = df.groupby([month_col, "StockCode"]).agg(
        net_gmv=("net_gmv", "sum")
    ).reset_index()

    df["rank"] = df.groupby(month_col)["net_gmv"].rank(method="first", ascending=False)

    topn = df[df["rank"] <= n].groupby(month_col).agg(
        topn_gmv=("net_gmv", "sum")
    ).reset_index()

    total = df.groupby(month_col).agg(
        total_gmv=("net_gmv", "sum")
    ).reset_index()

    merged = total.merge(topn, on=month_col, how="left")
    merged["topn_gmv"] = merged["topn_gmv"].fillna(0)
    merged[f"top{n}_coverage"] = (
        merged["topn_gmv"] / merged["total_gmv"]
    ).where(merged["total_gmv"] > 0, 0)

    return merged[[month_col, f"top{n}_coverage"]]


def sku_hhi(df: pd.DataFrame, month_col="month") -> pd.DataFrame:
    df = df.copy()
    df = df[df["type"] == "PRODUCT"]

    gmvsku = df.groupby([month_col, "StockCode"]).agg(
        net_gmv=("net_gmv", "sum")
    ).reset_index()

    total = gmvsku.groupby(month_col).agg(
        total_gmv=("net_gmv", "sum")
    ).reset_index()

    merged = total.merge(gmvsku, on=month_col, how="left")
    merged["net_gmv"] = merged["net_gmv"].fillna(0)
    merged["coverage"] = (
        merged["net_gmv"] / merged["total_gmv"]
    ).where(merged["total_gmv"] > 0, 0)

    def compute_hhi(s):
        return (s ** 2).sum()

    hhi = merged.groupby(month_col).agg(
        hhi=("coverage", compute_hhi)
    ).reset_index()

    return hhi


def monthly_kpis(df: pd.DataFrame, month_col="month") -> pd.DataFrame:
    df = df.copy()
    df_prod = df[df["type"] == "PRODUCT"].copy()

    g = df_prod.groupby(month_col).agg(
        net_gmv=("net_gmv", "sum"),
        order=("InvoiceNo", "nunique"),
        customer=("CustomerID", "nunique"),
        unit_sold=("Quantity", "sum"),
        avg_unit_price=("UnitPrice", "mean"),
        active_order_days=("order_date", "nunique")
    ).reset_index()

    df_return = df[df["is_return"] == True].groupby(month_col).agg(
        return_value=("return_value", "sum"),
        return_unit=("abs_quantity", "sum")
    ).reset_index()

    final = g.merge(df_return, on=month_col, how="left")
    final["return_value"] = final["return_value"].fillna(0)
    final["return_unit"] = final["return_unit"].fillna(0)

    total = final["net_gmv"] + final["return_value"].abs()
    final["return_rate"] = (final["return_value"].abs() / total).where(total > 0, 0)

    top20 = topn_coverage(df_prod, month_col, n=20)
    top50 = topn_coverage(df_prod, month_col, n=50)
    final = final.merge(top20, on=month_col, how="left")
    final = final.merge(top50, on=month_col, how="left")

    df_hhi = sku_hhi(df_prod, month_col)
    final = final.merge(df_hhi, on=month_col, how="left")

    dataset_end = df["InvoiceDate"].max()
    last_month = dataset_end.to_period("M")
    last_month_end_date = last_month.end_time.date()

    final["calendar_days_in_month"] = final[month_col].apply(
        lambda p: p.days_in_month if hasattr(p, "days_in_month") else pd.Period(p, freq="M").days_in_month
    )

    final["is_partial_month"] = False
    if dataset_end.date() < last_month_end_date:
        final.loc[final[month_col] == last_month, "is_partial_month"] = True

    final["net_gmv_per_active_day"] = (
        final["net_gmv"] / final["active_order_days"]
    ).where(final["active_order_days"] > 0, 0)

    final["product_sales_before_returns"] = final["net_gmv"]

    final = final.sort_values(month_col).reset_index(drop=True)
    return final