"""
SILVER LAYER — parsed, typed, deduplicated, quality-gated tables.

Design notes
------------
- silver_checks_parsed is a shared temporary view: dedup on
  (location_id, check_id, business_date) keeping the most recently ingested
  row, plus all type parsing and computed totals. Published tables derive
  from it so the logic exists exactly once.
- Expectations: DROP (@dlt.expect_all_or_drop) for null/invalid keys (routed
  to quarantine tables); WARN (@dlt.expect_all) for negative amounts/prices
  (kept with flag columns). silver_checks_quarantine,
  silver_check_items_quarantine, and silver_payments_quarantine re-apply the
  inverse predicates so every dropped row remains inspectable.
- IMPORTANT business rule: sum(SoldPrice*Quantity) vs sum(Payments.Amount)
  mismatch is an ANOMALY WE MEASURE (gold_payment_discrepancies), never a
  data-quality drop. No expectation filters on that comparison.
- Fact tables derive children (items / discounts / payments / voids) with a
  LEFT SEMI join on the DQ-filtered silver_checks, so referential integrity
  holds by construction.
- Large fact tables use LIQUID CLUSTERING on (business_date, location_id).

Published to: applebees.silver
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG = "applebees"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"

# ----------------------------------------------------------------------------------
# Shared parsed + deduplicated check base (temporary view, not published)
# ----------------------------------------------------------------------------------


def _sum_array(arr_col, amount_field):
    return F.aggregate(
        arr_col,
        F.lit(0.0),
        lambda acc, x: acc + F.coalesce(x[amount_field], F.lit(0.0)),
    )


@dlt.view(
    comment=(
        "Internal: bronze checks parsed to typed columns + computed totals, "
        "deduplicated on (LocationId, Id, BusinessDate) keeping the latest ingest."
    )
)
def silver_checks_parsed():
    checks = spark.read.table(f"{BRONZE}.bronze_checks")

    items_total = F.aggregate(
        "ItemsSold",
        F.lit(0.0),
        lambda acc, x: acc
        + F.coalesce(x["SoldPrice"], F.lit(0.0)) * F.coalesce(x["Quantity"], F.lit(0.0)),
    )
    item_disc_total = F.aggregate(
        "ItemsSold",
        F.lit(0.0),
        lambda acc, x: acc + _sum_array(F.coalesce(x["Discounts"], F.array()), "Amount"),
    )

    w = Window.partitionBy("LocationId", "Id", "BusinessDate").orderBy(
        F.col("_ingest_ts").desc(), F.col("_file_modification_time").desc()
    )

    return (
        checks.withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .select(
            F.col("Id").alias("check_id"),
            F.col("LocationId").alias("location_id"),
            F.col("BusinessDate").alias("business_date_raw"),
            F.to_date("BusinessDate").alias("business_date"),
            F.col("TableName").alias("table_name"),
            F.to_timestamp("OpenTime").alias("open_time"),
            F.to_timestamp("CloseTime").alias("close_time"),
            F.col("TrafficCount").alias("traffic_count"),
            F.col("ServiceCharge").alias("service_charge"),
            F.col("LoyaltyId").alias("loyalty_id"),
            F.col("RevenueCenterId").alias("revenue_center_id"),
            items_total.alias("items_total"),
            _sum_array(F.coalesce(F.col("Payments"), F.array()), "Amount").alias("payments_total"),
            _sum_array(F.coalesce(F.col("Tax"), F.array()), "Amount").alias("tax_total"),
            _sum_array(F.coalesce(F.col("Discounts"), F.array()), "Amount").alias("check_discount_total"),
            item_disc_total.alias("item_discount_total"),
            _sum_array(F.coalesce(F.col("Voids"), F.array()), "Amount").alias("void_total"),
            F.size(F.coalesce("ItemsSold", F.array())).alias("n_items"),
            F.size(F.coalesce("Payments", F.array())).alias("n_payments"),
            F.size(F.coalesce("Discounts", F.array())).alias("n_check_discounts"),
            F.size(F.coalesce("Voids", F.array())).alias("n_voids"),
            F.col("_ingest_ts"),
            F.coalesce(F.col("ItemsSold"), F.array()).alias("ItemsSold"),
            F.coalesce(F.col("Payments"), F.array()).alias("Payments"),
            F.coalesce(F.col("Discounts"), F.array()).alias("Discounts"),
            F.coalesce(F.col("Tax"), F.array()).alias("Tax"),
            F.coalesce(F.col("Voids"), F.array()).alias("Voids"),
            F.col("_file_path"),
            F.col("_dt"),
        )
    )


# ----------------------------------------------------------------------------------
# Published check-level table + quarantine
# ----------------------------------------------------------------------------------

CHECK_DQ_RULES = {
    "valid_check_id": "check_id IS NOT NULL AND check_id > 0",
    "valid_location_id": "location_id IS NOT NULL AND location_id > 0",
    "valid_business_date": "business_date IS NOT NULL",
}


@dlt.table(
    name=f"{SILVER}.silver_checks",
    comment=(
        "One row per valid check: parsed timestamps, computed totals "
        "(items, payments, tax, both discount channels, voids), deduplicated. "
        "Note: items_total != payments_total is intentionally NOT filtered — "
        "it is measured in gold_payment_discrepancies."
    ),
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(CHECK_DQ_RULES)
def silver_checks():
    return spark.read.table("silver_checks_parsed").drop(
        "business_date_raw", "_file_path", "_dt",
        "ItemsSold", "Payments", "Discounts", "Tax", "Voids",
    )


@dlt.table(
    name=f"{SILVER}.silver_checks_quarantine",
    comment=(
        "Checks dropped by silver_checks expectations (invalid id, location, "
        "or unparseable business date), with the failed rule(s) recorded. "
        "DQ evidence — nothing is silently lost."
    ),
    table_properties={"quality": "silver"},
)
def silver_checks_quarantine():
    parsed = spark.read.table("silver_checks_parsed")
    failed = (
        F.col("check_id").isNull()
        | (F.col("check_id") <= 0)
        | F.col("location_id").isNull()
        | (F.col("location_id") <= 0)
        | F.col("business_date").isNull()
    )
    return (
        parsed.filter(failed)
        .withColumn(
            "failed_rules",
            F.concat_ws(",", F.array(*[
                F.when(F.col("check_id").isNull() | (F.col("check_id") <= 0), F.lit("valid_check_id")),
                F.when(F.col("location_id").isNull() | (F.col("location_id") <= 0), F.lit("valid_location_id")),
                F.when(F.col("business_date").isNull(), F.lit("valid_business_date")),
            ])),
        )
        .filter(F.col("failed_rules") != "")
    )


# ----------------------------------------------------------------------------------
# Child fact tables (semi-joined to DQ-filtered checks for referential integrity)
# ----------------------------------------------------------------------------------


def _semi_join_valid_checks(df):
    valid = spark.read.table(f"{SILVER}.silver_checks").select(
        "check_id", "location_id", "business_date"
    )
    return df.join(valid, ["check_id", "location_id", "business_date"], "left_semi")


@dlt.table(
    name=f"{SILVER}.silver_check_items",
    comment="One row per sold item line with check context. Liquid clustered.",
    table_properties={"quality": "silver"},
    cluster_by=["business_date", "location_id"],
)
@dlt.expect_all_or_drop(
    {
        "valid_quantity": "quantity IS NOT NULL",
        "valid_sold_price": "sold_price IS NOT NULL",
        "valid_item_id": "item_id IS NOT NULL AND item_id > 0",
    }
)
@dlt.expect_all(
    {
        "warn_negative_sold_price": "sold_price >= 0",
        "warn_negative_quantity": "quantity >= 0",
    }
)
def silver_check_items():
    raw = (
        spark.read.table("silver_checks_parsed")
        .select(
            "check_id",
            "location_id",
            "business_date",
            F.explode(F.coalesce("ItemsSold", F.array())).alias("item"),
        )
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("item.ItemId").alias("item_id"),
        F.col("item.ItemName").alias("item_name"),
        F.col("item.ItemPLU").alias("item_plu"),
        F.col("item.Quantity").alias("quantity"),
        F.col("item.GrossPrice").alias("gross_price"),
        F.col("item.SoldPrice").alias("sold_price"),
        (F.coalesce(F.col("item.SoldPrice"), F.lit(0.0)) * F.coalesce(F.col("item.Quantity"), F.lit(0.0))).alias("line_total"),
        F.col("item.EmployeeId").alias("employee_id"),
        F.col("item.RevenueCenterId").alias("revenue_center_id"),
        F.to_timestamp(F.col("item.OrderTime")).alias("order_time"),
        F.col("item.Seat").alias("seat"),
        F.col("item.Sequence").alias("sequence"),
        F.col("item.ParentSequence").alias("parent_sequence"),
        F.col("item.PosSourceId").alias("pos_source_id"),
        F.col("item.ItemMajorCatId").alias("item_major_cat_id"),
        F.col("item.ItemMajorCatName").alias("item_major_cat_name"),
        F.col("item.ItemSubCatId").alias("item_sub_cat_id"),
        F.col("item.ItemSubCatName").alias("item_sub_cat_name"),
        F.size(F.coalesce(F.col("item.Discounts"), F.array())).alias("n_item_discounts"),
        (F.col("item.SoldPrice") < 0).alias("is_negative_sold_price"),
        (F.col("item.Quantity") < 0).alias("is_negative_quantity"),
    )
    return _semi_join_valid_checks(out)


@dlt.table(
    name=f"{SILVER}.silver_check_items_quarantine",
    comment=(
        "Items dropped by silver_check_items expectations (null sold_price, "
        "null quantity, or invalid item_id), with the failed rule(s) recorded. "
        "DQ evidence — nothing is silently lost."
    ),
    table_properties={"quality": "silver"},
)
def silver_check_items_quarantine():
    raw = (
        spark.read.table("silver_checks_parsed")
        .select(
            "check_id",
            "location_id",
            "business_date",
            F.explode(F.coalesce("ItemsSold", F.array())).alias("item"),
        )
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("item.ItemId").alias("item_id"),
        F.col("item.ItemName").alias("item_name"),
        F.col("item.Quantity").alias("quantity"),
        F.col("item.SoldPrice").alias("sold_price"),
    )
    failed = (
        F.col("quantity").isNull()
        | F.col("sold_price").isNull()
        | F.col("item_id").isNull()
        | (F.col("item_id") <= 0)
    )
    return (
        out.filter(failed)
        .withColumn(
            "failed_rules",
            F.concat_ws(
                ",", F.array(*[
                    F.when(F.col("quantity").isNull(), F.lit("valid_quantity")),
                    F.when(F.col("sold_price").isNull(), F.lit("valid_sold_price")),
                    F.when(
                        F.col("item_id").isNull() | (F.col("item_id") <= 0),
                        F.lit("valid_item_id"),
                    ),
                ])),
        )
        .filter(F.col("failed_rules") != "")
    )


@dlt.table(
    name=f"{SILVER}.silver_item_discounts",
    comment=(
        "One row per item-level discount with full check + item context, "
        "approver (EmployeeId), manager (ManagerId) and self_approved flag. "
        "Liquid clustered."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["business_date", "location_id"],
)
@dlt.expect_all_or_drop(
    {
        "valid_discount_amount": "discount_amount IS NOT NULL",
        "valid_discount_id": "discount_id IS NOT NULL",
    }
)
@dlt.expect_all(
    {"warn_negative_discount_amount": "discount_amount >= 0"}
)
def silver_item_discounts():
    raw = spark.read.table("silver_checks_parsed").select(
        "check_id",
        "location_id",
        "business_date",
        F.explode(F.coalesce("ItemsSold", F.array())).alias("item"),
    )
    exploded = (
        raw.select(
            "check_id",
            "location_id",
            "business_date",
            F.col("item.ItemId").alias("item_id"),
            F.col("item.ItemName").alias("item_name"),
            F.col("item.EmployeeId").alias("server_employee_id"),
            F.col("item.SoldPrice").alias("sold_price"),
            F.col("item.Quantity").alias("quantity"),
            F.explode(F.coalesce(F.col("item.Discounts"), F.array())).alias("d"),
        )
        .select(
            "check_id",
            "location_id",
            "business_date",
            "item_id",
            "item_name",
            "server_employee_id",
            "sold_price",
            "quantity",
            F.col("d.DiscountId").alias("discount_id"),
            F.col("d.DiscountName").alias("discount_name"),
            F.col("d.Amount").alias("discount_amount"),
            F.col("d.EmployeeId").alias("approver_employee_id"),
            F.col("d.ManagerId").alias("manager_id"),
        )
        .filter(F.col("discount_id").isNotNull())  # drop items with no discount
    )
    out = exploded.withColumn(
        "is_negative_discount_amount",
        (F.col("discount_amount") < 0),
    ).withColumn(
        "self_approved",
        F.col("approver_employee_id").isNotNull()
        & (F.col("approver_employee_id") > 0)
        & (F.col("approver_employee_id") == F.col("manager_id")),
    )
    return _semi_join_valid_checks(out)


@dlt.table(
    name=f"{SILVER}.silver_check_discounts",
    comment="One row per check-level discount with approver and manager context.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(
    {
        "valid_discount_amount": "discount_amount IS NOT NULL",
        "valid_discount_id": "discount_id IS NOT NULL",
    }
)
@dlt.expect_all(
    {"warn_negative_discount_amount": "discount_amount >= 0"}
)
def silver_check_discounts():
    raw = spark.read.table("silver_checks_parsed").select(
        "check_id",
        "location_id",
        "business_date",
        F.explode(F.coalesce("Discounts", F.array())).alias("d"),
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("d.DiscountId").alias("discount_id"),
        F.col("d.DiscountName").alias("discount_name"),
        F.col("d.Amount").alias("discount_amount"),
        F.col("d.EmployeeId").alias("approver_employee_id"),
        F.col("d.ManagerId").alias("manager_id"),
    ).filter(F.col("discount_id").isNotNull())
    out = out.withColumn(
        "is_negative_discount_amount",
        (F.col("discount_amount") < 0),
    ).withColumn(
        "self_approved",
        F.col("approver_employee_id").isNotNull()
        & (F.col("approver_employee_id") > 0)
        & (F.col("approver_employee_id") == F.col("manager_id")),
    )
    return _semi_join_valid_checks(out)


@dlt.table(
    name=f"{SILVER}.silver_payments",
    comment=(
        "One row per payment with method flags and gratuity. Liquid clustered."
    ),
    table_properties={"quality": "silver"},
    cluster_by=["business_date", "location_id"],
)
@dlt.expect_all_or_drop({"valid_payment_amount": "amount IS NOT NULL"})
@dlt.expect_all({"warn_negative_amount": "amount >= 0"})
def silver_payments():
    raw = spark.read.table("silver_checks_parsed").select(
        "check_id",
        "location_id",
        "business_date",
        F.explode(F.coalesce("Payments", F.array())).alias("p"),
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("p.PaymentCategoryId").alias("payment_category_id"),
        F.col("p.PayCatNo").alias("pay_cat_no"),
        F.upper(F.trim(F.col("p.PayCatName"))).alias("pay_cat_name"),
        F.col("p.Amount").alias("amount"),
        F.col("p.Gratuity").alias("gratuity"),
        F.col("p.EmployeeId").alias("employee_id"),
        F.col("p.PosSourceId").alias("pos_source_id"),
        F.coalesce(F.col("p.IsCreditCard"), F.lit(False)).alias("is_credit_card"),
        F.coalesce(F.col("p.IsGiftCard"), F.lit(False)).alias("is_gift_card"),
        F.coalesce(F.col("p.IsGiftCert"), F.lit(False)).alias("is_gift_cert"),
        F.coalesce(F.col("p.IsPettyCash"), F.lit(False)).alias("is_petty_cash"),
        F.coalesce(F.col("p.IsNonPOS"), F.lit(False)).alias("is_non_pos"),
        F.coalesce(F.col("p.IsCashDebit"), F.lit(False)).alias("is_cash_debit"),
        (F.col("p.Amount") < 0).alias("is_negative_amount"),
    )
    return _semi_join_valid_checks(out)


@dlt.table(
    name=f"{SILVER}.silver_payments_quarantine",
    comment=(
        "Payments dropped by silver_payments expectations (null amount), "
        "with the failed rule(s) recorded. DQ evidence."
    ),
    table_properties={"quality": "silver"},
)
def silver_payments_quarantine():
    raw = spark.read.table("silver_checks_parsed").select(
        "check_id",
        "location_id",
        "business_date",
        F.explode(F.coalesce("Payments", F.array())).alias("p"),
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("p.PaymentCategoryId").alias("payment_category_id"),
        F.col("p.PayCatNo").alias("pay_cat_no"),
        F.upper(F.trim(F.col("p.PayCatName"))).alias("pay_cat_name"),
        F.col("p.Amount").alias("amount"),
    )
    return (
        out.filter(F.col("amount").isNull())
        .withColumn(
            "failed_rules",
            F.lit("valid_payment_amount"),
        )
    )


@dlt.table(
    name=f"{SILVER}.silver_voids",
    comment="One row per void with check context and void details. Liquid clustered.",
    table_properties={"quality": "silver"},
    cluster_by=["business_date", "location_id"],
)
def silver_voids():
    raw = spark.read.table("silver_checks_parsed").select(
        "check_id",
        "location_id",
        "business_date",
        F.explode(F.coalesce("Voids", F.array())).alias("v"),
    )
    out = raw.select(
        "check_id",
        "location_id",
        "business_date",
        F.col("v.ItemId").alias("void_item_id"),
        F.col("v.Quantity").alias("void_quantity"),
        F.col("v.Amount").alias("void_amount"),
        F.col("v.EmployeeId").alias("void_employee_id"),
        F.col("v.ManagerId").alias("void_manager_id"),
        F.col("v.VoidCategoryId").alias("void_category_id"),
        F.col("v.VoidCategoryName").alias("void_category_name"),
        F.col("v.ItemName").alias("void_item_name"),
        F.col("v.ItemPLU").alias("void_item_plu"),
    )
    return _semi_join_valid_checks(out)


# ----------------------------------------------------------------------------------
# Silver dimensions
# ----------------------------------------------------------------------------------


@dlt.table(
    name=f"{SILVER}.silver_locations",
    comment="Typed location dimension.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({"valid_location_id": "location_id IS NOT NULL AND location_id > 0"})
def silver_locations():
    return (
        spark.read.table(f"{BRONZE}.bronze_locations")
        .select(
            F.col("Id").alias("location_id"),
            F.col("Name").alias("location_name"),
            F.to_date("OpenDate").alias("open_date"),
            F.to_date("ClosedDate").alias("closed_date"),
            F.col("TimeZoneName").alias("timezone_name"),
        )
    )


@dlt.table(
    name=f"{SILVER}.silver_employees",
    comment="Typed employee dimension.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({"valid_employee_id": "employee_id IS NOT NULL AND employee_id > 0"})
def silver_employees():
    return (
        spark.read.table(f"{BRONZE}.bronze_employees")
        .select(
            F.col("Id").alias("employee_id"),
            F.col("Name").alias("employee_name"),
            F.col("LocationId").alias("location_id"),
            F.col("HomeLocationId").alias("home_location_id"),
            F.col("LocationEmployeeId").alias("location_employee_id"),
            F.col("PayrollEmployeeId").alias("payroll_employee_id"),
            F.coalesce(F.col("Deleted"), F.lit(False)).alias("is_deleted"),
            F.coalesce(F.col("Borrowed"), F.lit(False)).alias("is_borrowed"),
            F.to_date("HireDate").alias("hire_date"),
            F.to_date("DateOfBirth").alias("date_of_birth"),
            F.coalesce(F.col("IsMinor"), F.lit(False)).alias("is_minor"),
        )
    )


@dlt.table(
    name=f"{SILVER}.silver_items",
    comment="Typed item dimension.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop({"valid_item_id": "item_id IS NOT NULL AND item_id > 0"})
def silver_items():
    return (
        spark.read.table(f"{BRONZE}.bronze_items").select(
            F.col("Id").alias("item_id"),
            F.col("Name").alias("item_name"),
            F.col("Plu").alias("plu"),
            F.col("SalesCategoryId").alias("sales_category_id"),
        )
    )


@dlt.table(
    name=f"{SILVER}.silver_revenue_centers",
    comment="Typed revenue-center dimension.",
    table_properties={"quality": "silver"},
)
@dlt.expect_all_or_drop(
    {"valid_revenue_center_id": "revenue_center_id IS NOT NULL AND revenue_center_id > 0"}
)
def silver_revenue_centers():
    return (
        spark.read.table(f"{BRONZE}.bronze_revenue_centers").select(
            F.col("Id").alias("revenue_center_id"),
            F.col("Name").alias("revenue_center_name"),
        )
    )
