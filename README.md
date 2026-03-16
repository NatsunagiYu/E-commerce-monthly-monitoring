# Monthly Ecommerce Performance Monitoring

## What this project does

This project monitors monthly ecommerce performance using transaction-level retail data.

It focuses on three main questions:

1. Has sales scale changed?
2. Is return burden or product risk showing unusual signals?
3. Is identified-customer health showing potential risk?

Python is used to clean data, classify transactions, calculate metrics, and generate outputs.  
Power BI is used as the final dashboard layer.

## Primary analyses

- Product Sales (before returns)
- Return Burden
- Top50 SKU Coverage
- SKU HHI
- Identified Sales Coverage
- Next-Month Retention
- Top10 Customer Share
- Customer HHI
- Top Return SKUs

## Supporting analyses

- MoM variance
- Cohort output

These supporting analyses are kept in Python outputs and documentation, but are not promoted into the dashboard’s main pages.

## Dashboard Preview

### Executive Summary

![Executive Summary](<outputs/Dashboard%20(Powerbi)/Execution_Summary.png>)

### Customer Health

![Customer Health](<outputs/Dashboard%20(Powerbi)/Customer_Health.png>)

### Returns / Product Risk

![Returns / Product Risk](<outputs/Dashboard%20(Powerbi)/Return_Product_Risk.png>)

## Important business boundaries

- The project is monthly monitoring only.
- `2011-12` is treated as a partial month.
- `net_gmv` means **Product Sales (before returns)** in this project.
- Customer-level views apply to **identified customers only**.
- Top return SKU analysis is for **driver discovery**, not root-cause proof.
- Cohort output is supporting only.

## Final source of truth

The Python pipeline is the final source of truth.

Main files:

- `src/transforms/clean.py`
- `src/kpi/kpi_calculations.py`
- `src/customer/customer_analysis.py`
- `src/report/report_generator.py`
- `scripts/run_pipeline.py`

## Key outputs

- `outputs/tables/kpi_monthly.csv`
- `outputs/tables/customer_health_monthly.csv`
- `outputs/tables/top_return_skus.csv`
- `outputs/tables/dim_month.csv`
- `outputs/reports/BUSINESS_REPORT.pdf`

## Dashboard pages

- Executive Summary
- Customer Health
- Returns / Product Risk
