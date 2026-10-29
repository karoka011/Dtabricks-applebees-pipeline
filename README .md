# Applebee's POS Medallion Pipeline

A Lakeflow Spark Declarative Pipeline (SDP) that ingests Applebee's point-of-sale (POS) data into Unity Catalog and turns it into **sales reporting and fraud-detection analytics**. The pipeline follows the **medallion architecture** (Bronze -> Silver -> Gold) and produces an auditable data-quality trail - nothing is ever silently dropped.

**Business question it answers:** *Which servers, locations, and behaviors look like manual cash-handling fraud - cash payments paired with voids/discounts, self-approved discounts, payment discrepancies, and abnormal gratuity rates?*

---

## Repository Structure

```
applebees_pos_medallion_pipeline_93eebb1d/
|
+-- transformations/                      # Pipeline source code (registered as pipeline libraries)
|   +-- 01_bronze.py                      # Raw ingestion: Auto Loader + dimension snapshots
|   +-- 02_silver.py                      # Cleaning, typing, dedup, expectations + quarantine
|   +-- 03_gold.py                        # Business-ready analytics & fraud detection tables
|   +-- 04_dq.py                          # Data-quality evidence (row counts + rule violations)
|
+-- applebees_pos_medallion_dab/          # Declarative Automation Bundle (DAB)
    +-- README.md                         # <- you are here
    +-- databricks.yml                    # Bundle definition (targets: dev / production)
    +-- resources/
        +-- jobs/
        |   +-- daily_orchestration_job.yml   # Daily job that triggers the pipeline
        +-- pipelines/
            +-- lakeflow_pipeline.yml         # Pipeline config (catalog, compute, libraries)
```

### Folder purpose

| Folder | Purpose |
| --- | --- |
| `transformations/` | All table logic for the pipeline, organized by medallion layer. Every file is a pipeline library - table definitions live in code, reviewed in Git. |
| `resources/jobs/` | Lakeflow Jobs definitions: schedule and orchestration of the daily run. |
| `resources/pipelines/` | Lakeflow pipeline configuration: catalog/schema, serverless compute, Photon, library paths. |
| `databricks.yml` | Bundle entry point - deploy the same code to dev or production with one command. |

---

## Architecture

```
Volume (raw JSON)  -->  BRONZE  -->  SILVER  -->  GOLD
 /Volumes/applebees/    raw,         typed,       business
 data/dataset/          immutable    deduped      analytics
                                     + DQ rules
```

### Layer 1 - Bronze (`01_bronze.py`)
* `applebees.bronze.bronze_checks` - **streaming table** ingesting raw POS checks incrementally via Auto Loader. Original nested structure preserved - the raw source of truth.
* Dimension snapshots loaded from the source volume: `bronze_employees`, `bronze_items`, `bronze_locations`, `bronze_revenue_centers`.

### Layer 2 - Silver (`02_silver.py`)
Cleaned, typed, deduplicated one-per-entity tables guarded by **expectations**:
* `silver_checks` - one row per valid check: parsed timestamps, computed totals (items, payments, tax, both discount channels, voids). Deduplicated on `(LocationId, Id, BusinessDate)`, keeping the latest ingest.
* `silver_check_items` - one row per sold item line (~28.5M rows), liquid clustered.
* `silver_item_discounts` / `silver_check_discounts` - discount events with **approver and manager context** and a `self_approved` flag.
* Typed dimensions: `silver_employees`, `silver_items`, `silver_locations`.
* **Quarantine tables** (`silver_checks_quarantine`, `silver_check_items_quarantine`) - every row dropped by an expectation is saved *with the failed rule recorded*. Full DQ auditability.

> Design note: `items_total != payments_total` is **intentionally NOT filtered** in silver. It is measured downstream in `gold_payment_discrepancies` - anomalies are evidence, not noise to discard.

### Layer 3 - Gold (`03_gold.py`)
Business-ready analytics and the fraud-detection views:

| Table | What it shows |
| --- | --- |
| `gold_daily_sales` | Daily sales per location: check count, traffic, gross/net sales, tax, discounts, voids, avg check. Core reporting table. |
| `gold_payment_discrepancies` | Checks where item totals do not match payments by more than $0.01, with the responsible primary server. |
| `gold_cash_void_discount_checks` | **Key fraud pattern**: paid (at least partly) in CASH and contains voids or discounts. |
| `gold_discount_abuse` | Per employee x location discount usage across both channels; self-approved discounts isolated; per-location rank. |
| `gold_tip_outliers` | Employees whose gratuity rate is more than 2 sigma above their location's mean. |
| `gold_employee_risk` | Per employee x location z-scores vs. same-location peers; `flag_*` = z > 2; `composite_risk_score` = number of flagged metrics (0-6). |
| `gold_location_summary` | Lifetime rollup per location: activity window, volume, sales, discounts, discrepancies. |

### Layer 4 - Data Quality (`04_dq.py`)
* `applebees.gold.dq_summary` - one row per metric: row counts per table and violations per expectation rule, measured at source. Reviewers can verify exactly what the rules caught and dropped.

---

## How the Fraud Analytics Work

1. **Payment discrepancies** flag any check where what was sold does not match what was paid.
2. **Cash + void/discount** intersections isolate the classic manual cash-handling pattern (void a card sale, pocket the cash).
3. **Self-approved discounts** (approver == manager) are surfaced per employee.
4. **Peer z-scores** normalize every employee against same-location peers, so a busy location does not mask outliers; `composite_risk_score` ranks suspects 0-6.

---

## Deployment & Operations

* **Platform**: Databricks Lakeflow Spark Declarative Pipelines (SDP), serverless compute with **Photon** enabled, Unity Catalog (`applebees` catalog).
* **Run mode**: Triggered - the daily orchestration job in `resources/jobs/` kicks off the pipeline refresh.
* **Deploy with the bundle**:
  ```
  databricks bundle deploy --target dev          # development
  databricks bundle deploy --target production   # production
  ```
* **Run manually**: open the pipeline and start an update (incremental by default).
* **Current health**: latest updates COMPLETED with no errors; only a handful of rows quarantined by expectations (recorded in `silver_checks_quarantine` - by design).

---

## Data Quality Guarantees

* Every expectation failure is **quarantined, not deleted** - with the rule name recorded.
* `dq_summary` proves row counts in = counts out + quarantined, per table.
* Raw nested data is preserved in bronze, so any silver/gold number can be traced back to the source.
