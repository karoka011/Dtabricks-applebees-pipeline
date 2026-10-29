"""
GOLD LAYER — business aggregates + fraud analytics (ported from EDA).

Tables
------
- gold_daily_sales:            per business_date x location: volume, traffic, sales,
                               tax, discounts, avg check.
- gold_location_summary:       lifetime rollup per location (incl. discrepancy counts).
- gold_payment_discrepancies:  checks where sum(SoldPrice*Qty) != sum(Payments.Amount)
                               beyond a $0.01 rounding tolerance, with the responsible
                               (primary server) employee. Pure measurement, no filtering.
- gold_employee_risk:          per employee x location z-scores vs same-location peers
                               across discount/discrepancy metrics; flag z > 2;
                               composite risk score = number of flagged metrics.
- gold_discount_abuse:         discount usage per employee incl. SELF-APPROVED
                               discounts (approver == manager) and per-location ranking.
- gold_tip_outliers:           employees whose gratuity rate is > 2 sigma above their
                               location's mean.

All gold tables are materialized views; fraud methodology matches the EDA notebook
(Phase 1-5 red flags) but is computed with native Spark (window z-scores).

Published to: applebees.gold
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "applebees"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

PAYMENT_TOLERANCE = 0.01  # USD; below this, items vs payments is considered rounding
Z_FLAG_THRESHOLD = 2.0


# ----------------------------------------------------------------------------------
# Business aggregates
# ----------------------------------------------------------------------------------


@dlt.table(
    name=f"{GOLD}.gold_daily_sales",
    comment=(
        "Daily sales per location: check count, traffic, gross/net sales, tax, "
        "discounts, voids, avg check. Core reporting table."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["business_date"],
)
def gold_daily_sales():
    checks = spark.read.table(f"{SILVER}.silver_checks")
    return (
        checks.groupBy("business_date", "location_id")
        .agg(
            F.count("*").alias("n_checks"),
            F.sum("traffic_count").alias("traffic_count"),
            F.sum("items_total").alias("gross_sales"),
            F.sum("service_charge").alias("service_charge"),
            F.sum("tax_total").alias("tax_total"),
            (F.sum("check_discount_total") + F.sum("item_discount_total")).alias("discount_total"),
            F.sum("void_total").alias("void_total"),
            F.sum("payments_total").alias("payments_total"),
        )
        .withColumn(
            "net_sales", F.col("gross_sales") + F.coalesce(F.col("service_charge"), F.lit(0.0))
        )
        .withColumn("avg_check", F.round(F.col("net_sales") / F.col("n_checks"), 2))
    )


@dlt.table(
    name=f"{GOLD}.gold_payment_discrepancies",
    comment=(
        "Checks where sum(SoldPrice*Quantity) differs from sum(Payments.Amount) "
        f"by more than ${PAYMENT_TOLERANCE}, with the responsible primary server. "
        "An anomaly measurement — never filtered out of silver."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["business_date", "location_id"],
)
def gold_payment_discrepancies():
    checks = (
        spark.read.table(f"{SILVER}.silver_checks")
        .withColumn("diff", F.round(F.col("items_total") - F.col("payments_total"), 2))
        .filter(F.abs(F.col("diff")) > PAYMENT_TOLERANCE)
    )

    # Primary server = modal EmployeeId among the check's sold items
    item_emps = (
        spark.read.table(f"{SILVER}.silver_check_items")
        .filter(F.col("employee_id").isNotNull() & (F.col("employee_id") > 0))
        .groupBy("check_id", "location_id", "business_date", "employee_id")
        .agg(F.count("*").alias("n_lines"))
    )
    w = Window.partitionBy("check_id", "location_id", "business_date").orderBy(
        F.col("n_lines").desc(), F.col("employee_id").asc()
    )
    primary_server = (
        item_emps.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "check_id",
            "location_id",
            "business_date",
            F.col("employee_id").alias("primary_employee_id"),
        )
    )

    return checks.join(
        primary_server, ["check_id", "location_id", "business_date"], "left"
    ).select(
        "check_id",
        "location_id",
        "business_date",
        "table_name",
        "items_total",
        "payments_total",
        "diff",
        F.when(F.col("diff") > 0, F.lit("UNDERPAID")).otherwise(F.lit("OVERPAID")).alias("discrepancy_type"),
        "n_items",
        "n_payments",
        "primary_employee_id",
    )


@dlt.table(
    name=f"{GOLD}.gold_location_summary",
    comment="Lifetime rollup per location: activity window, volume, sales, discounts, discrepancies.",
    table_properties={"quality": "gold"},
)
def gold_location_summary():
    daily = spark.read.table(f"{GOLD}.gold_daily_sales")
    discrepancies = (
        spark.read.table(f"{GOLD}.gold_payment_discrepancies")
        .groupBy("location_id")
        .agg(
            F.count("*").alias("n_discrepancy_checks"),
            F.sum(F.abs("diff")).alias("discrepancy_abs_total"),
        )
    )
    locations = spark.read.table(f"{SILVER}.silver_locations").select(
        "location_id", "location_name", "timezone_name"
    )
    rollup = (
        daily.groupBy("location_id")
        .agg(
            F.min("business_date").alias("first_business_date"),
            F.max("business_date").alias("last_business_date"),
            F.countDistinct("business_date").alias("n_days"),
            F.sum("n_checks").alias("n_checks"),
            F.sum("traffic_count").alias("traffic_count"),
            F.sum("net_sales").alias("net_sales"),
            F.sum("tax_total").alias("tax_total"),
            F.sum("discount_total").alias("discount_total"),
            F.sum("void_total").alias("void_total"),
        )
        .join(discrepancies, "location_id", "left")
        .join(locations, "location_id", "left")
        .withColumn(
            "discount_rate",
            F.round(
                F.col("discount_total")
                / (F.col("net_sales") + F.col("discount_total")),
                4,
            ),
        )
    )
    return rollup


# ----------------------------------------------------------------------------------
# Fraud analytics
# ----------------------------------------------------------------------------------


@dlt.table(
    name=f"{GOLD}.gold_cash_void_discount_checks",
    comment=(
        "EDA FOCUS TABLE: checks that contain voids or discounts AND were paid "
        "(at least partly) in CASH — the manual cash-handling fraud pattern. "
        "One row per matching check, with the responsible primary server."
    ),
    table_properties={"quality": "gold"},
    cluster_by=["business_date", "location_id"],
)
def gold_cash_void_discount_checks():
    checks = (
        spark.read.table(f"{SILVER}.silver_checks")
        .filter(
            (F.col("void_total") > 0)
            | (F.col("check_discount_total") > 0)
            | (F.col("item_discount_total") > 0)
        )
        .select(
            "check_id",
            "location_id",
            "business_date",
            "items_total",
            "check_discount_total",
            "item_discount_total",
            "void_total",
            "payments_total",
        )
    )

    cash = (
        spark.read.table(f"{SILVER}.silver_payments")
        .filter(F.upper(F.col("pay_cat_name")) == F.lit("CASH"))
        .groupBy("check_id", "location_id", "business_date")
        .agg(
            F.count("*").alias("n_cash_payments"),
            F.round(F.sum("amount"), 2).alias("cash_total"),
        )
    )

    # Primary server = modal EmployeeId among the check's sold items
    item_emps = (
        spark.read.table(f"{SILVER}.silver_check_items")
        .filter(F.col("employee_id").isNotNull() & (F.col("employee_id") > 0))
        .groupBy("check_id", "location_id", "business_date", "employee_id")
        .agg(F.count("*").alias("n_lines"))
    )
    w = Window.partitionBy("check_id", "location_id", "business_date").orderBy(
        F.col("n_lines").desc(), F.col("employee_id").asc()
    )
    primary_server = (
        item_emps.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            "check_id",
            "location_id",
            "business_date",
            F.col("employee_id").alias("primary_employee_id"),
        )
    )

    employees = spark.read.table(f"{SILVER}.silver_employees").select(
        F.col("employee_id").alias("_emp_id"), "employee_name"
    )
    locations = spark.read.table(f"{SILVER}.silver_locations").select(
        "location_id", "location_name"
    )

    return (
        checks.join(cash, ["check_id", "location_id", "business_date"], "inner")
        .join(primary_server, ["check_id", "location_id", "business_date"], "left")
        .join(employees, F.col("primary_employee_id") == F.col("_emp_id"), "left")
        .drop("_emp_id")
        .join(locations, "location_id", "left")
        .withColumn("has_void", F.col("void_total") > 0)
        .withColumn(
            "discount_total", F.col("check_discount_total") + F.col("item_discount_total")
        )
        .select(
            "check_id",
            "location_id",
            "location_name",
            "business_date",
            "primary_employee_id",
            "employee_name",
            "items_total",
            "check_discount_total",
            "item_discount_total",
            "discount_total",
            "void_total",
            "has_void",
            "payments_total",
            "cash_total",
            "n_cash_payments",
        )
    )


def _zscore(df, metric_cols, partition="location_id"):
    """Add z_<metric> and flag_<metric> columns; z computed vs same-location peers."""
    w = Window.partitionBy(partition)
    out = df
    for m in metric_cols:
        avg = F.avg(F.col(m)).over(w)
        std = F.stddev_samp(F.col(m)).over(w)
        z = F.when(std > 0, (F.col(m) - avg) / std).otherwise(F.lit(0.0))
        out = out.withColumn(f"z_{m}", F.round(z, 3)).withColumn(
            f"flag_{m}", (z > Z_FLAG_THRESHOLD) & (F.col(m) > 0)
        )
    return out


@dlt.table(
    name=f"{GOLD}.gold_employee_risk",
    comment=(
        "Per employee x location z-scores vs same-location peers across discount "
        "and payment-discrepancy metrics. flag_* = z > 2 (top ~2.5% of peers); "
        "composite_risk_score = number of flagged metrics (0-6)."
    ),
    table_properties={"quality": "gold"},
)
def gold_employee_risk():
    item_disc = (
        spark.read.table(f"{SILVER}.silver_item_discounts")
        .filter(F.col("approver_employee_id").isNotNull() & (F.col("approver_employee_id") > 0))
        .groupBy("location_id", F.col("approver_employee_id").alias("employee_id"))
        .agg(
            F.count("*").alias("item_disc_count"),
            F.sum("discount_amount").alias("item_disc_amt"),
        )
    )
    check_disc = (
        spark.read.table(f"{SILVER}.silver_check_discounts")
        .filter(F.col("approver_employee_id").isNotNull() & (F.col("approver_employee_id") > 0))
        .groupBy("location_id", F.col("approver_employee_id").alias("employee_id"))
        .agg(
            F.count("*").alias("check_disc_count"),
            F.sum("discount_amount").alias("check_disc_amt"),
        )
    )
    mismatch = (
        spark.read.table(f"{GOLD}.gold_payment_discrepancies")
        .filter(F.col("primary_employee_id").isNotNull())
        .groupBy("location_id", F.col("primary_employee_id").alias("employee_id"))
        .agg(
            F.count("*").alias("mismatch_count"),
            F.sum(F.abs("diff")).alias("mismatch_amt"),
        )
    )

    base = (
        item_disc.join(check_disc, ["location_id", "employee_id"], "full_outer")
        .join(mismatch, ["location_id", "employee_id"], "full_outer")
    )
    metric_cols = [
        "item_disc_count",
        "item_disc_amt",
        "check_disc_count",
        "check_disc_amt",
        "mismatch_count",
        "mismatch_amt",
    ]
    base = base.fillna(0.0, subset=metric_cols)
    # keep only employees with at least one tracked activity
    base = base.filter(
        (F.col("item_disc_count") > 0)
        | (F.col("check_disc_count") > 0)
        | (F.col("mismatch_count") > 0)
    )

    scored = _zscore(base, metric_cols)
    flag_cols = [F.col(f"flag_{m}").cast("int") for m in metric_cols]
    composite = sum(flag_cols[1:], flag_cols[0])

    employees = spark.read.table(f"{SILVER}.silver_employees").select(
        F.col("employee_id").alias("_emp_id"), "employee_name"
    )

    return (
        scored.withColumn("composite_risk_score", composite)
        .withColumn(
            "risk_tier",
            F.when(composite >= 4, F.lit("HIGH"))
            .when(composite >= 2, F.lit("MEDIUM"))
            .when(composite == 1, F.lit("WATCH"))
            .otherwise(F.lit("NONE")),
        )
        .join(employees, F.col("employee_id") == F.col("_emp_id"), "left")
        .drop("_emp_id")
    )


@dlt.table(
    name=f"{GOLD}.gold_discount_abuse",
    comment=(
        "Per employee x location discount usage across BOTH channels, with "
        "self-approved discounts (approver == manager) isolated and a per-location "
        "rank by total discount amount."
    ),
    table_properties={"quality": "gold"},
)
def gold_discount_abuse():
    item_disc = (
        spark.read.table(f"{SILVER}.silver_item_discounts")
        .filter(F.col("approver_employee_id").isNotNull() & (F.col("approver_employee_id") > 0))
        .groupBy("location_id", F.col("approver_employee_id").alias("employee_id"))
        .agg(
            F.count("*").alias("item_disc_count"),
            F.sum("discount_amount").alias("item_disc_amt"),
            F.sum(F.col("self_approved").cast("int")).alias("self_approved_count"),
            F.sum(F.when(F.col("self_approved"), F.col("discount_amount"))).alias("self_approved_amt"),
            F.countDistinct("check_id").alias("item_disc_checks"),
        )
    )
    check_disc = (
        spark.read.table(f"{SILVER}.silver_check_discounts")
        .filter(F.col("approver_employee_id").isNotNull() & (F.col("approver_employee_id") > 0))
        .groupBy("location_id", F.col("approver_employee_id").alias("employee_id"))
        .agg(
            F.count("*").alias("check_disc_count"),
            F.sum("discount_amount").alias("check_disc_amt"),
            F.sum(F.col("self_approved").cast("int")).alias("self_approved_count_check"),
            F.sum(F.when(F.col("self_approved"), F.col("discount_amount"))).alias("self_approved_amt_check"),
        )
    )

    base = (
        item_disc.join(check_disc, ["location_id", "employee_id"], "full_outer")
        .fillna(
            0.0,
            subset=[
                "item_disc_count",
                "item_disc_amt",
                "self_approved_count",
                "self_approved_amt",
                "item_disc_checks",
                "check_disc_count",
                "check_disc_amt",
                "self_approved_count_check",
                "self_approved_amt_check",
            ],
        )
        .withColumn("total_disc_count", F.col("item_disc_count") + F.col("check_disc_count"))
        .withColumn("total_disc_amt", F.col("item_disc_amt") + F.col("check_disc_amt"))
        .withColumn(
            "self_approved_total",
            F.col("self_approved_count") + F.col("self_approved_count_check"),
        )
        .withColumn(
            "self_approved_amt_total",
            F.coalesce(F.col("self_approved_amt"), F.lit(0.0))
            + F.coalesce(F.col("self_approved_amt_check"), F.lit(0.0)),
        )
    )

    w = Window.partitionBy("location_id").orderBy(F.col("total_disc_amt").desc())
    ranked = base.withColumn("rank_in_location", F.row_number().over(w))

    employees = spark.read.table(f"{SILVER}.silver_employees").select(
        F.col("employee_id").alias("_emp_id"), "employee_name"
    )
    locations = spark.read.table(f"{SILVER}.silver_locations").select(
        "location_id", "location_name"
    )

    return (
        ranked.join(employees, F.col("employee_id") == F.col("_emp_id"), "left")
        .drop("_emp_id")
        .join(locations, "location_id", "left")
        .filter(F.col("total_disc_count") > 0)
    )


@dlt.table(
    name=f"{GOLD}.gold_tip_outliers",
    comment=(
        "Employees whose gratuity rate (gratuity / pre-tip payment) is more than "
        f"{Z_FLAG_THRESHOLD:.0f} sigma above their location's mean."
    ),
    table_properties={"quality": "gold"},
)
def gold_tip_outliers():
    # NOTE: Gratuity is stored as a NEGATIVE amount in the source POS export
    # (the EDA used abs(Gratuity)); normalize to positive here.
    pays = (
        spark.read.table(f"{SILVER}.silver_payments")
        .withColumn("_grat", F.abs(F.col("gratuity")))
        .filter(
            (F.col("_grat") > 0)
            & F.col("employee_id").isNotNull()
            & (F.col("employee_id") > 0)
            & (F.col("amount") > F.col("_grat"))
        )
        .withColumn("tip_pct", F.col("_grat") / (F.col("amount") - F.col("_grat")))
    )
    per_emp = pays.groupBy("location_id", "employee_id").agg(
        F.count("*").alias("n_tipped_payments"),
        F.avg("tip_pct").alias("avg_tip_pct"),
        F.sum("_grat").alias("total_gratuity"),
    )
    scored = _zscore(per_emp, ["avg_tip_pct"])

    employees = spark.read.table(f"{SILVER}.silver_employees").select(
        F.col("employee_id").alias("_emp_id"), "employee_name"
    )
    locations = spark.read.table(f"{SILVER}.silver_locations").select(
        "location_id", "location_name"
    )

    return (
        scored.join(employees, F.col("employee_id") == F.col("_emp_id"), "left")
        .drop("_emp_id")
        .join(locations, "location_id", "left")
    )
