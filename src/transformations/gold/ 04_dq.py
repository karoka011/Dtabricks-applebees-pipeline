"""
DATA QUALITY SUMMARY — presentation evidence table.

applebees.gold.dq_summary holds one row per (table, metric, value):
- row counts for every published table,
- violation counts for each expectation rule, measured at the SOURCE
  (i.e., how many rows each rule dropped from silver tables),
- duplicate-check-key count (rows removed by the dedup step),
- quarantine count.

Non-zero violation numbers are GOOD evidence: they show the checks actively
identify and handle quality issues rather than assuming clean data.

Published to: applebees.gold.dq_summary
"""

import dlt
from pyspark.sql import functions as F

CATALOG = "applebees"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"


def _metric(table: str, metric: str, df, agg_expr):
    return df.agg(
        F.lit(table).alias("table_name"),
        F.lit(metric).alias("metric"),
        agg_expr.cast("long").alias("value"),
    )


def _rows(table: str, df):
    return _metric(table, "rows", df, F.count(F.lit(1)))


@dlt.table(
    name=f"{GOLD}.dq_summary",
    comment=(
        "Data-quality evidence: row counts per table and violation counts per "
        "expectation rule (rows dropped), measured at source. One row per metric."
    ),
    table_properties={"quality": "gold"},
)
def dq_summary():
    b_checks = spark.read.table(f"{BRONZE}.bronze_checks")
    s_checks = spark.read.table(f"{SILVER}.silver_checks")
    s_items = spark.read.table(f"{SILVER}.silver_check_items")
    s_payments = spark.read.table(f"{SILVER}.silver_payments")
    s_idisc = spark.read.table(f"{SILVER}.silver_item_discounts")
    s_cdisc = spark.read.table(f"{SILVER}.silver_check_discounts")
    s_quarantine = spark.read.table(f"{SILVER}.silver_checks_quarantine")
    s_voids = spark.read.table(f"{SILVER}.silver_voids")
    s_items_quarantine = spark.read.table(f"{SILVER}.silver_check_items_quarantine")
    s_payments_quarantine = spark.read.table(f"{SILVER}.silver_payments_quarantine")
    g_daily = spark.read.table(f"{GOLD}.gold_daily_sales")
    g_disc = spark.read.table(f"{GOLD}.gold_payment_discrepancies")
    g_risk = spark.read.table(f"{GOLD}.gold_employee_risk")
    g_abuse = spark.read.table(f"{GOLD}.gold_discount_abuse")
    g_tips = spark.read.table(f"{GOLD}.gold_tip_outliers")
    g_cash_focus = spark.read.table(f"{GOLD}.gold_cash_void_discount_checks")

    frames = []
    add = frames.append

    # --- Row counts (bronze -> silver -> gold flow) ---
    add(_rows("bronze.bronze_checks", b_checks))
    add(_rows("silver.silver_checks", s_checks))
    add(_rows("silver.silver_check_items", s_items))
    add(_rows("silver.silver_payments", s_payments))
    add(_rows("silver.silver_item_discounts", s_idisc))
    add(_rows("silver.silver_check_discounts", s_cdisc))
    add(_rows("silver.silver_checks_quarantine", s_quarantine))
    add(_rows("silver.silver_voids", s_voids))
    add(_rows("silver.silver_check_items_quarantine", s_items_quarantine))
    add(_rows("silver.silver_payments_quarantine", s_payments_quarantine))
    add(_rows("gold.gold_daily_sales", g_daily))
    add(_rows("gold.gold_payment_discrepancies", g_disc))
    add(_rows("gold.gold_employee_risk", g_risk))
    add(_rows("gold.gold_discount_abuse", g_abuse))
    add(_rows("gold.gold_tip_outliers", g_tips))
    add(_rows("gold.gold_cash_void_discount_checks", g_cash_focus))

    # --- Rule violation counts, measured at source ---
    # DROP expectations (null/invalid keys) — rows removed from silver tables
    add(_metric(
        "silver.silver_checks", "dropped_valid_check_id", b_checks,
        F.count(F.when(F.col("Id").isNull() | (F.col("Id") <= 0), 1)),
    ))
    add(_metric(
        "silver.silver_checks", "dropped_valid_location_id", b_checks,
        F.count(F.when(F.col("LocationId").isNull() | (F.col("LocationId") <= 0), 1)),
    ))
    add(_metric(
        "silver.silver_checks", "dropped_valid_business_date", b_checks,
        F.count(F.when(F.to_date("BusinessDate").isNull(), 1)),
    ))
    add(_metric(
        "silver.silver_checks", "duplicate_check_keys_removed", b_checks,
        F.count(F.lit(1)) - F.countDistinct("LocationId", "Id", "BusinessDate"),
    ))
    add(_metric(
        "silver.silver_check_items", "dropped_valid_sold_price",
        b_checks.select("Id", "LocationId", "BusinessDate",
                        F.explode("ItemsSold").alias("i")),
        F.count(F.when(F.col("i.SoldPrice").isNull(), 1)),
    ))
    add(_metric(
        "silver.silver_check_items", "dropped_valid_quantity",
        b_checks.select(F.explode("ItemsSold").alias("i")),
        F.count(F.when(F.col("i.Quantity").isNull(), 1)),
    ))
    add(_metric(
        "silver.silver_payments", "dropped_valid_payment_amount",
        b_checks.select(F.explode("Payments").alias("p")),
        F.count(F.when(F.col("p.Amount").isNull(), 1)),
    ))
    add(_metric(
        "silver.silver_item_discounts", "dropped_valid_discount_amount",
        b_checks.select(
            F.explode("ItemsSold").alias("i")
        ).select(F.explode(F.col("i.Discounts")).alias("d")),
        F.count(F.when(F.col("d.Amount").isNull(), 1)),
    ))
    add(_metric(
        "silver.silver_check_discounts", "dropped_valid_discount_amount",
        b_checks.select(F.explode("Discounts").alias("d")),
        F.count(F.when(F.col("d.Amount").isNull(), 1)),
    ))

    # WARN expectations (negative values) — rows kept in silver tables with flag columns
    add(_metric(
        "silver.silver_check_items", "warn_negative_sold_price",
        b_checks.select("Id", "LocationId", "BusinessDate",
                        F.explode("ItemsSold").alias("i")),
        F.count(F.when(F.col("i.SoldPrice") < 0, 1)),
    ))
    add(_metric(
        "silver.silver_check_items", "warn_negative_quantity",
        b_checks.select(F.explode("ItemsSold").alias("i")),
        F.count(F.when(F.col("i.Quantity") < 0, 1)),
    ))
    add(_metric(
        "silver.silver_payments", "warn_negative_amount",
        b_checks.select(F.explode("Payments").alias("p")),
        F.count(F.when(F.col("p.Amount") < 0, 1)),
    ))
    add(_metric(
        "silver.silver_item_discounts", "warn_negative_discount_amount",
        b_checks.select(
            F.explode("ItemsSold").alias("i")
        ).select(F.explode(F.col("i.Discounts")).alias("d")),
        F.count(F.when(F.col("d.Amount") < 0, 1)),
    ))
    add(_metric(
        "silver.silver_check_discounts", "warn_negative_discount_amount",
        b_checks.select(F.explode("Discounts").alias("d")),
        F.count(F.when(F.col("d.Amount") < 0, 1)),
    ))

    # --- Measured anomalies (NOT dropped — handled as business signals) ---
    add(_metric(
        "gold.gold_payment_discrepancies", "anomaly_checks_with_payment_gap",
        spark.read.table(f"{SILVER}.silver_checks"),
        F.count(
            F.when(
                F.abs(F.col("items_total") - F.col("payments_total")) > 0.01, 1
            )
        ),
    ))

    add(_metric(
        "gold.gold_cash_void_discount_checks", "anomaly_cash_checks_with_voids",
        g_cash_focus,
        F.count(F.when(F.col("has_void"), 1)),
    ))

    out = frames[0]
    for f in frames[1:]:
        out = out.unionByName(f)
    return out
