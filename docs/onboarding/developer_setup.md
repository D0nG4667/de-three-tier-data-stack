# Developer Setup & Local Onboarding Guide

Welcome to the Bristol Air Quality data stack! This document provides step-by-step instructions to boot the complete multi-container pipeline on your local machine, run test suites, compile models, and inspect the dashboards.

---

## 1. Prerequisites

Ensure you have the following tools installed and running locally:
- **Docker & Docker Compose** (minimum Docker Compose v2.0+)
- **Python 3.11** (optional, for local development outside containers)
- **Astral uv** (optional, recommended package manager for lightning-fast python syncing)

---

## 2. Bootstrapping the Container Stack

The entire three-tier database stack runs in containerized environments. Follow these steps to initialize your environment:

1. **Configure Environment Variables**:
   Copy the template environment file to activate database passwords and settings:
   ```bash
   cp .env.example .env
   ```
   *(You can open `.env` to customize your database passwords if desired).*

2. **Launch Services**:
   Build and start the multi-container stack in detached mode:
   ```bash
   docker compose build
   docker compose up -d
   ```

Verify that the serving daemons are healthy and running:
```bash
docker compose ps
```
You should see:
- `db-raw` (PostgreSQL) - **Healthy** (No host ports exposed for security)
- `db-olap` (PostgreSQL) - **Healthy** (No host ports exposed for security)
- `db-nosql` (MongoDB) - **Healthy** (No host ports exposed for security)
- `dagster-webserver` - **Running** (port 3000 bound to loopback `127.0.0.1`)
- `pgadmin` / `mongo-express` - **Running** (ports 5050 and 8081 bound to loopback `127.0.0.1`)

> [!NOTE]
> In accordance with production-grade separation of concerns, the **data generator** (`generator`) and **ETL pipeline** (`pipeline`) containers are configured with the `manual` Compose profile. They will not show up in the default `docker compose ps` list since they run as discrete, ad-hoc execution jobs rather than background daemons.

---

## 3. Step-by-Step Data Operations

### Step 1: Seed the Raw Landing Database (`db-raw`)
To drop the raw tables, create the 23-column schema matching the UWE Bristol dataset, and import the CSV records (or run simulation if configured), run:
```bash
docker compose run --rm generator python -m src.generator
```
*(This simulates the source system log uploads, preparing `db-raw` for extraction. Once completed, you will see the `raw_readings` metadata populated in the Dagster UI).*

![Dagster Raw Readings Ingestion Monitor](../assets/dagster_raw_readings.png)

### Step 2: Run the ETL Pipeline (Terminal Option)
If you prefer running the pipeline directly from your CLI without using the Dagster UI, run:
```bash
docker compose run --rm pipeline python -m src.pipeline
```
*(This extracts chunks from `db-raw`, applies data quality validation, hashes records, loads the clean rows into the warehouse `db-olap`, runs dbt models, and replicates a BSON sample to `db-nosql`).*

### Step 3: Run the ETL Pipeline (Dagster UI Option)
1. Open the Dagster web UI: [http://localhost:3000](http://localhost:3000)
2. Go to **Overview** -> **Assets** or the **Lineage** tab.
3. Click the **"Reload definitions"** button in the top-right corner to make sure your workspace is in sync.
4. Click **"Materialize all"** to execute the pipeline with full observability.

![Dagster UI Workspace Console](../assets/dagster_deployment.png)

*Upon successful materialization of all assets, you will see a fully green-checked, connected pipeline representing your end-to-end data flow:*

![Materialized Dagster Lineage Graph](../assets/dagster_lineage_materialized.png)


---

## 4. Testing & Validation

### Run Automated Tests
To run unit and validation tests using `pytest` inside the development-targeted environment, execute:
```bash
docker compose run --rm dagster pytest tests/
```
*(Note: We run tests inside the `dagster` service container because it is configured with the `development` build stage, which bundles testing libraries like `pytest` and mounts the `/app/tests/` directory. The `pipeline` container targets `production` and does not include these dev packages).*

### Validate Dagster Orchestration Definitions
To verify that your Dagster pipeline graphs, dependencies, and translators are syntactically and logically correct:
```bash
docker compose run --rm dagster dagster definitions validate -f /app/dagster_orch/definitions.py
```

### Verify dbt Compilation
To test that your dbt staging and mart models compile correctly:
```bash
docker compose run --rm pipeline dbt compile --project-dir dbt_project --profiles-dir dbt_project
```

### Verify Analytical SQL Query Results
To run the conformed analytical queries directly against the materialized views inside the OLAP database and inspect the results, run:

#### Query A: Highest recorded NOx in 2019
```bash
docker compose exec db-olap psql -U postgres -d bristol_olap -c "SELECT * FROM query_a;"
```
*Expected Output:*
```text
      date_time      |  station_name  | highest_nox 
---------------------+----------------+-------------
 2019-01-24 09:00:00 | Colston Avenue |      1403.5
(1 row)
```

#### Query B: Commute PM2.5 averages for 2019 at 08:00
```bash
docker compose exec db-olap psql -U postgres -d bristol_olap -c "SELECT * FROM query_b;"
```
*Expected Output:*
```text
     station_name     |     mean_pm2_5     | mean_vpm2_5 
----------------------+--------------------+-------------
 AURN St Pauls        | 10.963870994506344 |            
 Brislington Depot    |                    |            
 Colston Avenue       |                    |            
 Fishponds Road       |                    |            
 Parson Street School | 11.870881795883179 |            
 Temple Way           |                    |            
 Wells Road           |                    |            
(7 rows)
```

#### Query C: Decadal Commute PM2.5 averages (2010–2019) at 08:00
```bash
docker compose exec db-olap psql -U postgres -d bristol_olap -c "SELECT * FROM query_c;"
```
*Expected Output:*
```text
           station_name           |     mean_pm2_5     |    mean_vpm2_5    
----------------------------------+--------------------+-------------------
 AURN St Pauls                    | 12.501938892143492 | 2.959303245414417
 Bath Road                        |                    |                  
 Brislington Depot                |                    |                  
 CREATE Centre Roof               |                    |                  
 Cheltenham Road \ Station Road   |                    |                  
 Colston Avenue                   |                    |                  
 Fishponds Road                   |                    |                  
 Newfoundland Road Police Station |                    |                  
 Old Market                       |                    |                  
 Parson Street School             | 11.870881795883179 |                  
 Rupert Street                    |                    |                  
 Shiner's Garage                  |                    |                  
 Temple Way                       |                    |                  
 Wells Road                       |                    |                  
(14 rows)
```

---

## 5. Local Dashboard Portals

Once the containers are up, you can access these local management dashboards:

| Service Portal | URL | Credentials |
|---|---|---|
| **Dagster Orchestrator** | [http://localhost:3000](http://localhost:3000) | *No authentication needed* |
| **dbt Docs Portal** | [http://localhost:8080](http://localhost:8080) | *No authentication needed* |
| **pgAdmin 4** (Postgres DBs) | [http://localhost:5050](http://localhost:5050) | Email: `admin@admin.com`<br>Password: `admin_password` |
| **Mongo Express** (MongoDB) | [http://localhost:8081](http://localhost:8081) | Username: `root`<br>Password: `mongo_root_password` |

---

## 6. Multi-Stage Docker Architecture

We utilize an enterprise-grade, **Multi-Stage Build Pattern** in [Dockerfile.pipeline](../../docker/Dockerfile.pipeline) to split our dependencies and build targets.

```mermaid
graph TD
    Stage1["Stage 1: base<br>(Slim python base + settings)"]
    Stage2["Stage 2: builder<br>(gcc, libpq-dev, git + uv sync)"]
    Stage3["Stage 3: production<br>(Slim runtime, no dev/test libraries)"]
    Stage4["Stage 4: development<br>(Includes dev deps, tests/, pytest run)"]

    Stage1 ──► Stage2
    Stage2 ──► Stage3
    Stage2 ──► Stage4

    style Stage3 fill:#d4edda,stroke:#28a745,stroke-width:2px;
    style Stage4 fill:#fff3cd,stroke:#ffc107,stroke-width:2px;
```


### Engineering Rationale & Benefits
1. **Zero Dev/Test Footprint in Prod**: 
   The `production` stage does not bundle testing binaries or files. This significantly reduces image size, increases startup speed in the cloud, and minimizes security vulnerabilities (by avoiding shipping testing packages like `pytest` or compiler tools to Dagster Cloud).
2. **Build Caching Performance**:
   Dependencies are resolved in `Stage 2` using only `pyproject.toml` and `uv.lock`. If you modify application source files in `src/` or `dagster_orch/`, Stage 2 remains cached, and Docker only rebuilds the final lightweight runtime layer in **under 10 seconds**.
3. **dbt Offline Manifest Parsing**:
   Because compiled dbt target folders are excluded from source control (via `.dockerignore`), the build file executes `RUN dbt parse` during the container image construction phase. This compiles and writes `manifest.json` directly into the image layer, preventing any runtime `DagsterDbtManifestNotFoundError` when code locations are initialized on Dagster Plus.

---

## 7. Documentation Portal & GitOps Confluence Sync

### Local dbt Docs Serving
To enable developers and analytics engineers to browse table schemas, descriptions, and structural lineage, the stack includes a live-updating `dbt-docs` container.

* **URL**: [http://localhost:8080](http://localhost:8080)
* **Start Service**:
  ```bash
  docker compose up -d dbt-docs
  ```
  *(This will generate the data catalog schema definitions, compile dependencies, and boot the web portal on port 8080)*.

### GitOps Confluence Integration Sync
To bridge code-driven engineering specs with business-facing wikis, we have established a **GitOps Documentation Sync Pipeline** using GitHub Actions:

* **Workflow Trigger**: Runs automatically on any push or merge events to the `main` branch affecting documentation files (`docs/**`).
* **Workflow Location**: [.github/workflows/confluence_sync.yml](../../.github/workflows/confluence_sync.yml)
* **Confluence Target Space**: Link to space wiki [uwe-bristol-air.atlassian.net/wiki](https://uwe-bristol-air.atlassian.net/wiki)
* **Required GitHub Secrets**:
  - `CONFLUENCE_SPACE_KEY`: Key of the target Confluence space (e.g. `BRISTOLAIR`).
  - `CONFLUENCE_EMAIL`: Account email for authentication.
  - `CONFLUENCE_API_TOKEN`: Atlassian developer API token.
  - `CONFLUENCE_PARENT_PAGE_ID`: ID of the parent page under which the wiki page directory tree is automatically mirrored.

