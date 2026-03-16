import pandas as pd

def classify_transaction(row):
    invoice = str(row["InvoiceNo"]).upper().strip()
    stock = str(row.get("StockCode", "")).upper().strip()
    des = str(row.get("Description", "")).upper().strip()
    qty = row.get("Quantity", 0)

    if stock == "AMAZONFEE" or "AMAZON FEE" in des:
        return "FEE"

    if stock == "CRUK" or "CRUK COMMISSION" in des:
        return "FEE"

    if stock == "BANK CHARGES" or "BANK CHARGES" in des:
        return "BANK_CHARGE"

    if stock == "M" or "MANUAL" in des:
        return "MANUAL"

    if stock == "D" or "DISCOUNT" in des:
        return "DISCOUNT"

    if "POSTAGE" in des:
        return "POSTAGE"

    if "SAMPLES" in des:
        return "SAMPLES"

    if invoice.startswith("C") or qty < 0:
        return "RETURN"

    return "PRODUCT"


def clean_data(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df = df.dropna(subset=["InvoiceNo", "StockCode", "UnitPrice", "Quantity"])
    df["InvoiceDate"] = pd.to_datetime(df["InvoiceDate"])
    df = df[df["UnitPrice"] > 0]

    df["type"] = df.apply(classify_transaction, axis=1)

    df["is_return"] = df["type"] == "RETURN"
    df["is_product"] = df["type"] == "PRODUCT"
    df["is_non_merch_adjustment"] = df["type"].isin(
        ["FEE", "BANK_CHARGE", "MANUAL", "DISCOUNT", "POSTAGE", "SAMPLES"]
    )

    df["line_gmv"] = df["UnitPrice"] * df["Quantity"]

    df["net_gmv"] = df["line_gmv"].where(df["is_product"] & (df["Quantity"] > 0), 0)

    df["return_value"] = df["line_gmv"].where(df["is_return"], 0)

    df["order_date"] = df["InvoiceDate"].dt.date
    df["month"] = df["InvoiceDate"].dt.to_period("M")
    df["abs_quantity"] = df["Quantity"].abs()

    return df