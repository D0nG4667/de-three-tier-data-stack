# System Topology

This document details the multi-container topology of the Bristol Air Quality data stack and describes how each tier maps to the medallion architecture stages.

---

## Data Flow Topology

The system operates across isolated Docker containers on a private network (`bristol-air-net`), ensuring secure, modular, and reproducible operations.

```mermaid
flowchart TD
    %% Tier 1: Ingestion (Bronze)
    subgraph T1_Ingest ["Tier 1: Ingestion & Raw Landing [Bronze]"]
        CSV[Air_Quality_Continuous.csv] -->|Pandas Chunk Reader| GEN[Ingestion Engine: generator.py]
        GEN -->|Seeds Raw Schema| RAW_DB[(db-raw: PostgreSQL 18)]
    end

    %% Tier 2: Storage & Transformation (Silver)
    subgraph T2_ETL ["Tier 2: Core ETL & Warehouse [Silver]"]
        RAW_DB -->|Keyset Pagination: 10k Chunks| ETL[ETL Pipeline: pipeline.py]
        ETL -->|Data Quality Gates: validate.py| DQ{Validation & Hashing}
        DQ -->|Passes Crop Timeline| CROP[Crop & Normalise: transform.py]
        CROP -->|Bulk SQL Inserts| OLAP_DB[(db-olap: PostgreSQL 18)]
        OLAP_DB -->|dbt compile & run| DBT[dbt transformations]
        DBT -->|Staging Views| STG_V[stg_constituencies, stg_readings, stg_stations]
        STG_V -->|Analytical Marts| MART_F[fact_reading]
        STG_V -->|Analytical Marts| MART_D[dim_station, dim_constituency]
    end

    %% Tier 3: Replication & Serving (Gold)
    subgraph T3_Serving ["Tier 3: Document Serving [Gold]"]
        MART_F & MART_D -->|Nested BSON Denormalization| NOSQL_SYNC[MongoDB replicator]
        NOSQL_SYNC -->|Sub-second Read Cache| NOSQL_DB[(db-nosql: MongoDB 8.3)]
        NOSQL_DB -->|Dashboard Serving| MONGO_Q[query_nosql.js]
    end
```

---

## Medallion Stage Mapping

Our three-database structure aligns with the **Medallion Architecture** to isolate storage, cleaning, and reporting concerns:

### 1. Bronze Layer (`db-raw`)
* **Purpose**: Serves as the raw municipal sensor landing area and historical audit trail.
* **Characteristics**: Retains raw, unvalidated telemetry records. Includes sensor dropouts (nulls), duplicate timestamps, and out-of-bounds metrics (negative readings) as received from the source.

### 2. Silver Layer (`db-olap`)
* **Purpose**: Cleansed, validated, and normalized corporate data warehouse.
* **Characteristics**:
  - Filtered to retain data from `2010-01-01` onwards.
  - Telemetry anomalies (out-of-bounds dates, invalid pollutant measurements) are removed by inline Python validation gates.
  - Integrity verified via MD5 row checksums (`row_checksum`).
  - Deduplicated using analytical SQL window functions in dbt staging models (`ROW_NUMBER() OVER (PARTITION BY site_id, date_time ORDER BY id) = 1`).
  - Structured into Third Normal Form (3NF) relational tables.

### 3. Gold Layer (`db-nosql`)
* **Purpose**: Performance-optimized serving layer for client dashboards.
* **Characteristics**: Denormalizes the relational schemas into nested BSON document models, eliminating multi-table JOIN latency to support sub-millisecond query returns.

---

## Live Lineage Observability (Dagster)

Below is the verified end-to-end lineage graph generated inside the Dagster web UI. It maps the dependencies starting from the raw database ingestion sensor (`raw_readings`) down to our three-output Python ETL multi-asset (`readings`, `stations`, `constituencies`), the downstream dbt transformations model chain, and finally the denormalized MongoDB replica cache:

![Materialized Dagster Lineage Graph](../assets/dagster_lineage_materialized.png)

