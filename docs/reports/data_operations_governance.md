---
confluence_page_id: "98607"
---

# Data Operations and Governance Specification
**Project**: Bristol Air Quality Three-Tier Data Stack  

---

## 1. Defensive Ingestion: Validation Gates
Operational data pipelines are exposed to dirty input data. Our `validate.py` module defines strict checks at the row-level during ETL execution.

### Schema Integrity Assertions:
1. **Critical Null Checks**: Rows missing `date_time` or `site_id` are immediately dropped.
2. **Physical Boundary Limits**:
   - **NOx limits**: $0.0 \leq \text{NO}_x \leq 2000.0\ \mu g/m^3$.
   - **Temperature limits**: $-20.0 \leq \text{temp} \leq 45.0\ \text{°C}$.
   - **Humidity limits**: $0.0 \leq \text{RH} \leq 100.0\ \%$.
3. **Numeric Conformance**: Iterates over 15 telemetry values to verify they parse into floats, preventing SQL execution failures.

---

## 2. Lineage and Audits: Cryptographic Checksums
To prevent silent mutations and guarantee data lineage consistency as records migrate from the OLTP stage (`db-raw`) through Python ETL to the OLAP serving warehouse (`db-olap`) and NoSQL Document Store (`db-nosql`), we compute an MD5 row checksum:

```python
def compute_row_checksum(row_dict):
    # Sort keys to ensure consistent hashing
    sorted_items = sorted([(str(k), str(v)) for k, v in row_dict.items() if k not in ['id', 'row_checksum']])
    hash_input = "".join([f"{k}:{v}" for k, v in sorted_items]).encode('utf-8')
    return hashlib.md5(hash_input).hexdigest()
```
*Governance Value*: Downstream BI analysts can compare row-level checksum hashes between relational and document databases to audit replication jobs and ensure zero-loss migration.

---

## 3. Observability and DataOps Monitoring (Dagster)
The pipeline's runtime telemetry is monitored through **Dagster**, providing a centralized dashboard for asset state and quality checks:
1. **Asset Metadata Tracking**: Important pipeline metrics (such as `warehouse_loaded_rows`, `dropped_anomalies_count`, and `telemetry_throughput_rows_per_second`) are logged dynamically to the Dagster UI on every execution run.
2. **Double-Handler Logging**: Runtime messages are split between the standard console log and a persistent file log (`/app/data/pipeline.log`) on the host.

### Failure Exceptions and Alerting
When a critical schema failure or database outage occurs, Dagster registers a step failure, blocking downstream assets (like `dbt_warehouse` and `mongodb_sample`) from executing on corrupted inputs. Alerts can be routed immediately to Slack or PagerDuty using Dagster Sensors or logs.

### dbt Schema Test Gates
The warehouse leverages an automated dbt test suite (defined in `sources.yml` and `marts/schema.yml`) to verify schema integrity on every run. In alignment with Medallion data quality standards:
- **Raw Ingestion Sources**: Are verified for null constraints (such as `date_time` and `row_checksum`) but are exempted from unique assertions since raw telemetry streams can naturally contain double-reads.
- **Analytical Marts**: The final conformed `fact_reading` table is validated using strict `unique` and `not_null` constraints on `reading_id` and `row_checksum`, guaranteeing that the warehouse remains clean and deduplicated.

---

## 4. Test-Driven Development (TDD)
Before deploying code changes to production, developers run unit tests inside the containerized environment:
- `test_compute_row_checksum()`: Assures key sorting and hash stability.
- `test_validate_row_schema_valid()` / `_invalid()`: Validates out-of-bounds metrics.
- `test_transform_batch_cropping()`: Verifies that pre-2010 and post-2022 records are dropped.
