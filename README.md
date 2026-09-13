# 🚀 Rearc Data Quest — Databricks Enterprise Edition

[![Databricks](https://shields.io|%20Unity%20Catalog-FF3621?logo=databricks&logoColor=white)](https://databricks.com)
[![Apache Spark](https://shields.io)](https://apache.org)
[![Framework](https://shields.io|%20Delta%20Live%20Tables-00a2ed?logo=databricks&logoColor=white)](https://databricks.com)
[![Language](https://shields.io|%20SQL-3776AB?logo=python&logoColor=white)](https://apache.orgdocs/latest/api/python/index.html)

This repository contains a production-grade, highly scalable, and fully automated **End-to-End Medallion Architecture Pipeline**. It integrates multi-decade quarterly macroeconomic time-series data from the Bureau of Labor Statistics (BLS) with annual national population demographics sourced from the DataUSA API.

The entire infrastructure lifecycle, data orchestration jobs, and distributed Delta Live Tables (DLT) networks are declared explicitly as code via **Databricks Asset Bundles (DABs)**.

---

## 🏛️ 1. Complete Architecture & Topology Blueprint

The data lakehouse architecture separates concerns across three logical layers. It isolates lightweight raw ingestion from heavy parallel compute shuffles, ensuring data consistency and strict governance inside the Unity Catalog namespace:

```mermaid
graph TD
    %% Source Layer
    subgraph External Data Sources
        SRC1[BLS Web Server Index<br>time.series/pr/]
        SRC2[DataUSA Population API<br>Tesseract OLAP Cube]
    end

    %% Storage Landing
    subgraph Storage Ingestion Layer
        V1[(Unity Catalog Volume:<br>rearc_landing_zone)]
    end

    %% Bronze Layer
    subgraph Staging Layer [Bronze Tables — Appends Only]
        B1[bronze_bls_data]
        B2[bronze_bls_series]
        B3[bronze_bls_sector]
        B4[bronze_bls_measure]
        B5[bronze_bls_period]
        B6[bronze_bls_duration]
        B7[bronze_population]
    end

    %% Silver Layer
    subgraph Conformed Layer [Silver Tables — Cleansed & Typed]
        S1[silver_bls_data]
        S2[silver_bls_series]
        S3[silver_bls_sector]
        S4[silver_bls_measure]
        S5[silver_population]
        M1[🌟 silver_enriched_bls_metadata]
    end

    %% Gold Layer
    subgraph Presentation Layer [Gold Tables — Business Intelligence Ready]
        G1[gold_us_population_stats]
        G2[gold_top_productivity_years]
        G3[gold_productivity_vs_population_master]
        G4[gold_productivity_quarterly_trends]
        G5[gold_sector_benchmarking_summary]
    end

    %% Data Pipeline Connections
    SRC1 -->|Python Sync Engine| V1
    SRC2 -->|Python Sync Engine| V1

    V1 --> B1 & B2 & B3 & B4 & B5 & B6 & B7

    B1 -->|Header Trim & Cast| S1
    B2 -->|Header Trim & Clean| S2
    B3 -->|Header Trim & Clean| S3
    B4 -->|Header Trim & Clean| S4
    B7 -->|Array Explode & Cast| S5

    S2 & S3 & S4 -->|Wide Star Schema Pre-Join| M1

    S5 -->|Filter 2013-2018| G1
    S1 -->|Window Row Rank| G2
    M1 -->|Inner Join Context| G2
    S1 -->|Global Join Mapping| G3
    S5 -->|Chronological Left Join| G3
    S1 -->|Window Lag Metric| G4
    S1 & M1 -->|Grouped Multi-Agg| G5

    %% Component Visual Styles
    classDef bronze fill:#f9cb9c,stroke:#333,stroke-width:1px;
    classDef silver fill:#cfe2f3,stroke:#333,stroke-width:1px;
    classDef gold fill:#d9ead3,stroke:#333,stroke-width:1px;
    class B1,B2,B3,B4,B5,B6,B7 bronze;
    class S1,S2,S3,S4,S5,M1 silver;
    class G1,G2,G3,G4,G5 gold;
```

---

## 🗂️ 2. Repository Layout

```text
rearc-data-quest/
├── databricks.yml              # Main Databricks Asset Bundle definition configuration
├── PROCESS.md                  # Detailed architectural trade-offs, rationale, & retrospectives
├── README.md                   # Execution blueprint and portfolio walkthrough (This file)
├── src/                        # Data Pipeline Engineering codebase
│   ├── step1_ingestion.py      # Pure Python incremental syncing web crawler
│   └── rearc_dlt_pipeline.py   # PySpark Lakeflow / Delta Live Tables framework declaration
└── screenshots/                # Visual proof files (DLT Graph execution & output matrices)
```

---

## ⚙️ 3. Operational Layer In-Depth

### 📦 1. The Ingestion Engine & Bronze Staging
*   **Dynamic Scraping & Zero Hardcoding:** `src/step1_ingestion.py` executes lightweight HTML index parsing using `BeautifulSoup4`. If the BLS server gains, alters, or drops files, the sync engine adapts programmatically without code adjustments.
*   **Bypassing the HTTP 403 Firewall:** Injects browser user-agent tokens bundled with real contact parameters to conform with the official **BLS Data Access Policy**, maintaining high pipeline uptime.
*   **File-Level Idempotency Guard:** Fires lightweight HTTP `HEAD` pre-checks to evaluate the remote server's `Content-Length` bytes. If an identical file exists in the landing zone volume (`rearc_landing_zone`), the download step is skipped, saving server bandwidth.
*   **Schema & Drift Resistance:** To neutralize uneven spacing padding standard inside raw government metadata fields (e.g. `series_id        `), the Bronze layer executes a programmatic string-cleaning loop that runs `.strip()` across all headers dynamically during ingestion.
*   **Delta Column Mapping Protection:** Enables native column mapping variables (`delta.columnMapping.mode = name`) on JSON entities to protect Spark clusters from crashing when handling names with invalid nested symbols or whitespace (like `Nation ID`).

### 🥈 2. The Cleansing & Conformed Silver Tier
*   **Strict Quality Control (Expectations):** Implements automated data governance boundaries (`@dlt.expect_or_drop`) to purge corrupted rows right at the processing gate (`series_id IS NOT NULL`, `population > 0`).
*   **Wide Dimensional Enrichment (`silver_enriched_bls_metadata`):** To minimize expensive network data-shuffling across distributed compute nodes, the low-cardinality metadata datasets (`series`, `sector`, `measure`) are pre-joined in memory at the Silver layer. This wide-star dimension allows downstream tables to pull clear human-readable context fields via one single join step.

### 🏆 3. The Business Presentation Gold Tier
Satisfies all primary core asks, analytical queries, and bonus questions using **PySpark DataFrames** paired with fully documented alternative structures in **Spark SQL**:
*   `gold_us_population_stats`: Extracts explicit mean and standard deviation baselines for the US population strictly between 2013 and 2018 inclusive.
*   `gold_top_productivity_years`: Uses distributed Spark window partitions (`F.row_number().over()`) to isolate the single highest performing year for every unique economic industry code, enriched with clear text titles.
*   `gold_productivity_vs_population_master`: A completely unbounded master matrix joining the comprehensive BLS timeline side-by-side with national census metrics. Missing time gaps automatically resolve to a clean `null` value using chronologically ordered `LEFT JOIN` logic.
*   `gold_productivity_quarterly_trends`: **[BONUS]** Employs historical analytic lag filters (`F.lag()`) to determine Quarter-over-Quarter (QoQ) percentage growth shifts.
*   `gold_sector_benchmarking_summary`: **[BONUS]** Provides macro summaries (floors, peaks, averages) categorized by global industry name bands for executive monitoring.

---

## 🚀 4. How to Deploy and Run via Databricks Asset Bundles

Because this project relies completely on Infrastructure as Code (IaC), there is no need to manually click through the web UI interface.

### 1. Install & Authenticate the Databricks CLI
Ensure the Databricks CLI tool is present on your machine, then authorize connection tokens targeting your assigned workspace instance:
```bash
databricks auth login --host https://dbc-a1d25508-ad4e.cloud.databricks.com
```

### 2. Validate and Deploy the Pipeline Project
Run the automated bundle validator to check for structural consistency, then deploy all configurations live to your workspace:
```bash
# Validate code formatting and cluster configurations
databricks bundle validate

# Deploy notebooks, cluster resources, and workflow jobs instantly
databricks bundle deploy -t dev
```

### 3. Run the Sequential Workflows
Trigger the master workflow job execution remotely. Databricks will spin up a single-node cluster, download files to the landing Volume via Task 1, and immediately kick off the Delta Live Tables computation network via Task 2:
```bash
databricks bundle run rearc_orchestration_job
```

---

## 👥 5. Self-Service Access Layers & Security Governance

To bridge the gap between backend engineering and business teams, the following access control layers have been configured on top of the Gold tables:

1.  **Unity Catalog Access Control Lists (ACLs):** Enforced a strict **Principle of Least Privilege** using ANSI SQL commands. We provisioned a custom group (`read_only_analysts`) with explicit read-only `SELECT` capabilities targeting the Gold reporting layer. Because no privileges are granted on the raw landing Volume, Bronze, or Silver tables, internal operational layers remain securely hidden.
2.  **Databricks Genie Space Data Assistant:** Configured a conversational AI exploration agent. Backed by plain-text constraints (such as mapping the phrase *"First Quarter"* to `'Q01'`), non-technical business managers can query the entire dataset using natural English text phrases.
