# Rent-Airbnb Data Pipeline

A production-ready ETL pipeline implementing the **medallion architecture** (Bronze → Silver → Gold) to process and analyze rental property data from Airbnb and rental listing sources.

## Architecture Overview

This pipeline implements a three-layer data lakehouse architecture in **Unity Catalog** (`rentalproperty` catalog) using Delta Lake tables. Each layer serves a distinct purpose in the data quality and transformation journey.

---

## 🥉 Bronze Layer: Raw Data Ingestion

**Notebook:** `001_bronze_ingest`

**Purpose:** Capture raw data exactly as it arrives from source systems with minimal transformation. The bronze layer serves as the **immutable source of truth** and historical archive.

**Why Bronze?**
* **Data lineage** — Preserve original data for auditing and reprocessing
* **Recovery** — Reprocess downstream layers without re-ingesting from external sources
* **Schema evolution** — Store raw data before understanding all business rules
* **Compliance** — Maintain unmodified records for regulatory requirements
* **Debugging** — Trace data quality issues back to original source

**Tables:**
* `rentalproperty.bronze.airbnb` — Raw Airbnb listings (CSV)
* `rentalproperty.bronze.rentals` — Raw rental listings (JSON)

**Key Characteristics:**
* Minimal transformation (type inference only)
* Includes ingestion timestamp (`ingestedDateTime`)
* Overwrite mode (full refresh pattern)
* Preserves all source columns unchanged

---

## 🥈 Silver Layer: Cleaned & Standardized Data

**Notebooks:** `002_silver_load_airbnb`, `003_silver_load_rentals`

**Purpose:** Transform raw data into **clean, conformed, and enriched** datasets ready for business use. The silver layer applies data quality rules, standardizes formats, and adds business keys.

**Why Silver?**
* **Data quality** — Remove duplicates, handle nulls, validate formats
* **Standardization** — Normalize naming conventions (camelCase), date formats, postal codes
* **Consistency** — Apply uniform business rules across all datasets
* **Performance** — Optimized schema and types for downstream queries
* **Reusability** — Single source of clean data for multiple use cases
* **Enrichment** — Add derived columns and business keys without changing bronze

**Tables:**
* `rentalproperty.silver.airbnb`
* `rentalproperty.silver.rentals`

**Key Transformations:**
* **Deduplication** — Remove duplicate records
* **Type casting** — Convert strings to proper types (int, double, timestamp)
* **Postal code validation** — Normalize Dutch postal codes to `1234 AB` format
* **Currency parsing** — Clean currency fields, handle NA/None/null values
* **Date parsing** — Extract structured dates from free-text availability fields
* **Primary key generation** — Create `listingPk` via MD5 hash of coordinates
* **Audit columns** — Add `insertedDatetime`, `updatedDatetime`, `source`
* **Column renaming** — Standardize to camelCase with descriptive names
* **Drop obsolete columns** — Remove crawl metadata not needed for analytics

---

## 🥇 Gold Layer: Business-Level Aggregates

**Notebook:** `004_gold_load_revenue`

**Purpose:** Create **highly aggregated, business-specific datasets** optimized for reporting, dashboards, and analytics. The gold layer answers specific business questions with pre-computed metrics.

**Why Gold?**
* **Performance** — Pre-aggregated data for instant dashboard response
* **Simplicity** — Business users query simple tables, not complex joins
* **Consistency** — Metrics calculated once, used everywhere (single version of truth)
* **Cost optimization** — Avoid repeated expensive aggregations
* **Business alignment** — Tables map directly to business KPIs and reports

**Tables:**
* `rentalproperty.gold.airbnb_per_listing` — Total revenue by listing
* `rentalproperty.gold.airbnb_per_postalcode` — Total revenue by postal code (Airbnb)
* `rentalproperty.gold.rental_per_listing` — Total revenue by rental listing
* `rentalproperty.gold.rental_per_postalcode` — Total revenue by postal code (rentals)

**Key Aggregations:**
* Revenue summaries grouped by listing or postal code
* Includes geographic coordinates for geospatial analysis
* Audit columns for tracking data freshness
* Optimized for BI tool consumption

---

## Pipeline Orchestration

**Notebook:** `005_runnable`

Executes the full ETL pipeline sequentially:
1. Bronze ingestion
2. Silver transformation (Airbnb + Rentals)
3. Gold aggregation

Uses `dbutils.notebook.run()` to orchestrate the pipeline with proper error handling and logging.

---

## Data Flow

```
Source Files                Bronze Layer              Silver Layer             Gold Layer
─────────────              ──────────────            ──────────────           ──────────────
airbnb.csv       ──▶  bronze.airbnb       ──▶  silver.airbnb     ──▶  gold.airbnb_per_listing
                                                                    └─▶  gold.airbnb_per_postalcode

rentals.json     ──▶  bronze.rentals      ──▶  silver.rentals    ──▶  gold.rental_per_listing
                                                                    └─▶  gold.rental_per_postalcode
```

---

## Benefits of Medallion Architecture

1. **Separation of concerns** — Each layer has a single, well-defined purpose
2. **Incremental complexity** — Transformations become more sophisticated layer-by-layer
3. **Flexible reprocessing** — Rebuild silver/gold without re-ingesting bronze
4. **Performance optimization** — Each layer optimized for its access pattern
5. **Data governance** — Clear boundaries for data quality rules and ownership
6. **Schema evolution** — Changes isolated to specific layers
7. **Cost efficiency** — Reduce redundant processing via pre-aggregation

---

## Some Future Enhancements 

* Implement **SCD Type 1** or **Type 2** for tracking historical changes and to have a backfill operation for improve source data
* Switch from overwrite to **incremental merge** patterns
* Implement **data lineage tracking** with metadata catalogs
* Add **automated testing** for transformation logic
* Deploy to production using **Declarative Automation Bundles (DABs)** and **Workflow Jobs** for enterprise-grade orchestration, version control, and CI/CD integration
