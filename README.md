# retailpulse
## 1. Project Overview

RetailPulse is an end-to-end retail data engineering project built using **Databricks, Apache Spark, Delta Lake, and Unity Catalog**.

The project processes retail data from multiple sources and transforms it into clean, structured datasets that can be used for business analysis and reporting.

The pipeline follows the **Medallion Architecture**, consisting of three layers:

- **Bronze:** Raw data ingestion.
- **Silver:** Data cleaning and transformation.
- **Gold:** Business-ready datasets and aggregations.

## 2. Technology Stack

- Databricks Free Edition
- Apache Spark (PySpark)
- Delta Lake
- Unity Catalog
- Lakeflow Declarative Pipelines
- Auto Loader
- Python
- Git and GitHub

## 3. Project Architecture

```text
Retail Data Generator
        |
        v
   Landing Layer
        |
        v
   Bronze Layer
 (Raw data ingestion)
        |
        v
   Silver Layer
 (Cleaning and transformation)
        |
        v
    Gold Layer
 (Business aggregations)
        |
        v
 Analytics and Reporting
```

## 4. Unity Catalog Structure

**Catalog:** `workspace`

The project uses the following schemas:

| Schema | Purpose |
|---|---|
| `rp_landing` | Stores incoming source files and operational metadata |
| `rp_bronze` | Stores ingested raw Delta tables |
| `rp_silver` | Stores cleaned and transformed datasets |
| `rp_gold` | Stores business-ready datasets |

### Volumes

The landing schema is used to organize source data and operational metadata.

- `raw`: Stores incoming retail source data.
- `ops`: Intended for operational files such as Auto Loader checkpoints and schema metadata.

The current setup uses an `ops` folder inside the `raw` volume. The intended structure is to maintain separate managed volumes for `raw` and `ops`.

## 5. Repository Structure

```text
RetailPulse/
│
├── notebooks/
│
├── pipelines/
│   └── transformations/
│       └── transformation_file.py
│
├── sql/
│
├── tests/
│
├── docs/
│   ├── design_notes.md
│   └── m0_git_smoke_test.md
│
└── README.md
```

## 6. Source Datasets

The project uses retail datasets generated through `retail_data_generator.py`.

| Dataset | Format | Description |
|---|---|---|
| stores | CSV | Store information |
| products | CSV | Product and SKU information |
| sales_daily | JSON Lines | Daily sales transactions |
| sales_history | CSV | Historical weekly sales |
| forecast_runs | CSV | Forecast information |
| planner_overrides | JSON Lines | Planner override events |

## 7. Naming Conventions

- Schemas use the prefix `rp_`.
- Bronze tables use descriptive dataset names.
- Silver tables represent cleaned datasets.
- Gold tables represent business-level outputs.
- Checkpoints are stored under `ops/chk/<table>`.
- Auto Loader schema metadata is stored under `ops/schemas/<dataset>`.

## 8. Databricks Free Edition Considerations

This project is developed using Databricks Free Edition. The implementation accounts for its limitations:

- Serverless compute is used.
- Streaming pipelines use supported `availableNow` processing where applicable.
- Managed Unity Catalog volumes and tables are used.
- Checkpoint and schema metadata locations are explicitly configured.
- Source code is maintained in GitHub.

## 9. Project Objectives

The project demonstrates practical data engineering concepts, including:

- Building data ingestion pipelines.
- Working with Auto Loader.
- Implementing Medallion Architecture.
- Transforming data using PySpark.
- Managing data with Unity Catalog.
- Building Lakeflow pipelines.
- Maintaining code using Git and GitHub.
