import pandas as pd

def mom_variance(df: pd.DataFrame, month_col="month") -> pd.DataFrame:
    df = df.copy()
    df = df.sort_values(month_col).reset_index(drop=True)

    compare_list = [
        "net_gmv",
        "return_rate",
        "top20_coverage",
        "top50_coverage",
        "hhi"
    ]

    for x in compare_list:
        df[f"{x}_mom"] = df[x].pct_change()

    return df