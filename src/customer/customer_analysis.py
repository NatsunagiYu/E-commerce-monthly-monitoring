import pandas as pd

def create_customer_set ( s : pd.Series) -> set:

    unique_customer = s.dropna().unique()
    return set(unique_customer)

def retention (df : pd.DataFrame, month_col = "month") -> pd.DataFrame:
    
    df = df.copy()
    df = df[df['type']=='PRODUCT']

    time_window_set = df.groupby(month_col)['CustomerID'].apply(create_customer_set).reset_index()
    
    time_window_set.columns = [month_col, 'customer_set']
    time_window_set = time_window_set.sort_values(month_col).reset_index(drop=True)

    result = []

    for i in range(len(time_window_set)-1):

        current_month = time_window_set.loc[i,month_col]
        current_c = time_window_set.loc[i,'customer_set']
        next_c = time_window_set.loc[i+1,'customer_set']

        base = len(current_c)
        repeat = len(current_c & next_c)

        repeat_rate = repeat / base if base > 0 else 0

        result.append({
            month_col: current_month,
            "current_customers": base,
            "repeat_customers": repeat,
            "next_month_repeat_rate": repeat_rate
        })


    return pd.DataFrame(result)

def customer_concentration (df : pd.DataFrame, month_col ="month", n = 50) -> pd.DataFrame:

    df = df.copy()

    df = df[df['type'] == 'PRODUCT']

    cust_rev = df.groupby(['CustomerID',month_col]).agg(
        cust_gmv = ("net_gmv","sum")
    ).reset_index()

    total = cust_rev.groupby(month_col).agg(
        cust_total_gmv = ("cust_gmv","sum")
    ).reset_index()

    cust_rev['rank'] = cust_rev.groupby(month_col)['cust_gmv'].rank( method="first", ascending = False)

    topn = cust_rev[cust_rev['rank'] <= n ].groupby(month_col).agg(
        topn_gmv = ("cust_gmv","sum")
    ).reset_index()

    final = total.merge(topn, on=month_col, how="left")
    final['topn_gmv'] = final['topn_gmv'].fillna(0)
    final[f"top{n}_share"] = (final['topn_gmv'] / final['cust_total_gmv']).where(final['cust_total_gmv'] > 0, 0)


    cust_rev = cust_rev.merge(total, on=month_col, how="left")
    cust_rev['share'] = (cust_rev['cust_gmv']/ cust_rev['cust_total_gmv']).where(cust_rev['cust_total_gmv'] > 0, 0)

    hhi = cust_rev.groupby(month_col).agg(
        customer_hhi = ("share", lambda s: (s **2).sum())
    ).reset_index()

    final = final.merge(hhi, on=month_col, how="left")
    final = final.sort_values(month_col).reset_index(drop=True)

    return final[[month_col,f"top{n}_share","customer_hhi"]]


def cohort_table(df: pd.DataFrame, month_col: str = "month", freq: str = "M") -> pd.DataFrame:
    data = df[df["type"] == "PRODUCT"].copy()
    data["order_month"] = data[month_col]

    def parse_period(series: pd.Series, freq: str) -> pd.PeriodIndex:
        s = series.astype(str).str.strip()

        if freq == "M":
            return pd.PeriodIndex(s, freq="M")
        elif freq == "Q":
            s = s.str.replace("-", "", regex=False)
            return pd.PeriodIndex(s, freq="Q")
        elif freq == "Y":
            return pd.PeriodIndex(s, freq="Y")
        else:
            raise ValueError("freq must be one of ['M', 'Q', 'Y']")

    data["order_month"] = parse_period(data["order_month"], freq)

    first_purchase = (
        data.groupby("CustomerID")["order_month"]
        .min()
        .reset_index()
        .rename(columns={"order_month": "cohort_month"})
    )

    data = data.merge(first_purchase, on="CustomerID", how="left")

    if freq == "M":
        data["cohort_index"] = (
            (data["order_month"].dt.year - data["cohort_month"].dt.year) * 12
            + (data["order_month"].dt.month - data["cohort_month"].dt.month)
        )
    elif freq == "Q":
        data["cohort_index"] = (
            (data["order_month"].dt.year - data["cohort_month"].dt.year) * 4
            + (data["order_month"].dt.quarter - data["cohort_month"].dt.quarter)
        )
    elif freq == "Y":
        data["cohort_index"] = (
            data["order_month"].dt.year - data["cohort_month"].dt.year
        )

    cohort = (
        data.groupby(["cohort_month", "cohort_index"])["CustomerID"]
        .nunique()
        .reset_index()
        .rename(columns={"CustomerID": "customer_count"})
    )

    cohort["cohort_month"] = cohort["cohort_month"].astype(str)

    return cohort

def customer_id_coverage(df: pd.DataFrame, month_col="month") -> pd.DataFrame:
    data = df[df["type"] == "PRODUCT"].copy()
    data["is_identified"] = data["CustomerID"].notna()

    total = data.groupby(month_col).agg(
        total_product_rows=("InvoiceNo", "size"),
        total_product_orders=("InvoiceNo", "nunique"),
        total_product_sales=("net_gmv", "sum")
    ).reset_index()

    identified = data[data["is_identified"]].groupby(month_col).agg(
        identified_product_rows=("InvoiceNo", "size"),
        identified_product_orders=("InvoiceNo", "nunique"),
        identified_product_sales=("net_gmv", "sum")
    ).reset_index()

    out = total.merge(identified, on=month_col, how="left").fillna(0)

    out["identified_row_coverage"] = (
        out["identified_product_rows"] / out["total_product_rows"]
    ).where(out["total_product_rows"] > 0, 0)

    out["identified_order_coverage"] = (
        out["identified_product_orders"] / out["total_product_orders"]
    ).where(out["total_product_orders"] > 0, 0)

    out["identified_sales_coverage"] = (
        out["identified_product_sales"] / out["total_product_sales"]
    ).where(out["total_product_sales"] > 0, 0)

    return out.sort_values(month_col).reset_index(drop=True)

def top_return_skus(df: pd.DataFrame, month_col="month", top_n=10) -> pd.DataFrame:
    data = df[df["type"] == "RETURN"].copy()

    sku_returns = (
        data.groupby([month_col, "StockCode", "Description"], dropna=False)
        .agg(
            return_value=("return_value", "sum"),
            return_units=("abs_quantity", "sum")
        )
        .reset_index()
    )

    sku_returns["return_burden"] = sku_returns["return_value"].abs()

    sku_returns = sku_returns.sort_values(
        [month_col, "return_burden"],
        ascending=[True, False]
    )

    sku_returns["rank"] = sku_returns.groupby(month_col)["return_burden"].rank(
        method="first",
        ascending=False
    )

    top_returns = sku_returns[sku_returns["rank"] <= top_n].copy()

    return top_returns[
        [month_col, "StockCode", "Description", "return_burden", "return_units", "rank"]
    ].reset_index(drop=True)