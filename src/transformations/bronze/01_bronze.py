"""
BRONZE LAYER — raw ingestion for the Applebee's POS medallion pipeline.

- bronze_checks: one row per POS check. Raw nested JSON structure is preserved
  (ItemsSold[], Payments[], Discounts[], Tax[], Voids[]) plus ingestion metadata.
  Ingestion is INCREMENTAL via Auto Loader: only new dt=YYYY-MM-DD files arriving
  in the source volume are processed; previously ingested files are never re-read.
  Files are JSON ARRAYS (not JSON lines), so we read them with the binaryFile
  format (1 row per file) and parse the array with from_json — this is the
  supported incremental path for multi-line JSON.
- bronze_locations / bronze_employees / bronze_items / bronze_revenue_centers:
  small dimension snapshots, fully refreshed on every pipeline update.

Published to: applebees.bronze
"""

import dlt
from pyspark.sql import functions as F

CATALOG = "applebees"
SOURCE_VOLUME = "/Volumes/applebees/data/dataset"

# Exact nested schema of a check record (verified against the raw files).
# from_json ignores unknown fields and yields NULL for missing ones, so new
# optional fields in future files will not break ingestion.
CHECK_SCHEMA_DDL = '''array<struct<
    Id: bigint,
    LocationId: bigint,
    BusinessDate: string,
    TableName: string,
    OpenTime: string,
    CloseTime: string,
    TrafficCount: double,
    ServiceCharge: double,
    LoyaltyId: string,
    RevenueCenterId: bigint,
    Tax: array<struct<TaxCategoryId: bigint, Amount: double, TaxCategoryName: string>>,
    Discounts: array<struct<
        Amount: double, DiscountId: bigint, DiscountName: string,
        EmployeeId: bigint, ManagerId: bigint>>,
    Payments: array<struct<
        PaymentCategoryId: bigint, Amount: double, Gratuity: double,
        EmployeeId: bigint, PosSourceId: bigint, PayCatNo: bigint,
        PayCatName: string, IsCreditCard: boolean, IsGiftCard: boolean,
        IsGiftCert: boolean, IsPettyCash: boolean, IsNonPOS: boolean,
        IsCashDebit: boolean>>,
    ItemsSold: array<struct<
        ItemId: bigint, Quantity: double, GrossPrice: double, SoldPrice: double,
        EmployeeId: bigint, RevenueCenterId: bigint, OrderTime: string,
        Seat: bigint, Sequence: bigint, ParentSequence: bigint,
        PosSourceId: bigint, ItemName: string, ItemPLU: bigint,
        ItemMajorCatId: bigint, ItemMajorCatName: string,
        ItemSubCatId: bigint, ItemSubCatName: string,
        Discounts: array<struct<
            Amount: double, DiscountId: bigint, DiscountName: string,
            EmployeeId: bigint, ManagerId: bigint>>>>,
    Voids: array<struct<
        ItemId: bigint, Quantity: double, Amount: double, EmployeeId: bigint,
        ManagerId: bigint, VoidCategoryId: bigint, VoidCategoryName: string,
        ItemName: string, ItemPLU: bigint>>
>>'''


@dlt.table(
    name=f"{CATALOG}.bronze.bronze_checks",
    comment=(
        "Raw POS checks (original nested structure preserved) ingested "
        "incrementally from the source volume with Auto Loader. One row per check."
    ),
    table_properties={"quality": "bronze"},
)
def bronze_checks():
    files = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "binaryFile")  # 1 row per file -> handles JSON arrays
        .option("pathGlobFilter", "checks*.json")  # exclude dimensions/*.json
        .option("cloudFiles.includeExistingFiles", "true")
        .option("cloudFiles.maxFilesPerTrigger", "10")  # bounded micro-batches during backfill
        .load(SOURCE_VOLUME)
    )

    parsed = files.select(
        F.col("path").alias("_file_path"),
        F.col("modificationTime").alias("_file_modification_time"),
        F.from_json(F.col("content").cast("string"), CHECK_SCHEMA_DDL).alias("checks"),
    )

    # explode_outer: an unparseable/empty file still yields one all-NULL check row,
    # which downstream expectations route to quarantine instead of vanishing.
    return (
        parsed.select(
            "_file_path",
            "_file_modification_time",
            F.regexp_extract("_file_path", r"dt=([0-9]{4}-[0-9]{2}-[0-9]{2})", 1).alias("_dt"),
            F.explode_outer("checks").alias("check"),
        )
        .select(
            "check.*",
            "_file_path",
            "_file_modification_time",
            "_dt",
            F.current_timestamp().alias("_ingest_ts"),
        )
    )


# --- Dimension snapshots (small files; full refresh each update) ------------

DIMENSIONS = {
    "bronze_locations": f"{SOURCE_VOLUME}/dimensions/locations/locations.json",
    "bronze_employees": f"{SOURCE_VOLUME}/dimensions/employees/employees.json",
    "bronze_items": f"{SOURCE_VOLUME}/dimensions/items/items.json",
    "bronze_revenue_centers": f"{SOURCE_VOLUME}/dimensions/revenue_centers/revenueCenters.json",
}


def _register_dimension(table_name: str, path: str):
    @dlt.table(
        name=f"{CATALOG}.bronze.{table_name}",
        comment=f"Raw {table_name} dimension snapshot read from {path}.",
        table_properties={"quality": "bronze"},
    )
    def _dim(path=path):  # bind path per iteration (avoid late-binding closure bug)
        return (
            spark.read.format("json")
            .option("multiLine", "true")
            .load(path)
            .withColumn("_ingest_ts", F.current_timestamp())
        )


for _name, _path in DIMENSIONS.items():
    _register_dimension(_name, _path)
