import os
import time
import yaml
import psycopg2
from pathlib import Path
from typing import Any, Mapping
from dagster_orch.project import dbt_project
from dagster_dbt import DbtCliResource, dbt_assets, DagsterDbtTranslator
from dagster import asset, Output, AssetExecutionContext, MetadataValue, AssetKey, multi_asset, AssetOut

from src.transform import transform_batch
from src.extract import extract_raw_data_in_batches
from src.load import setup_olap_schema, seed_olap_static_tables, load_clean_batch_to_olap, load_sample_to_mongodb

CONFIG_PATH = os.getenv("CONFIG_PATH", str(Path(__file__).resolve().parent.parent / "config" / "config.yaml"))

def load_config() -> dict:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

@asset(group_name="ingestion")
def raw_readings(context: AssetExecutionContext) -> Output[int]:
    """
    Asset representing the raw staging database containing telemetry records.
    Verifies connection to the raw relational database and counts available rows.
    """
    config = load_config()
    raw_db_conf = config.get("raw_db", {})
    
    conn = psycopg2.connect(
        host=os.getenv("RAW_DB_HOST", raw_db_conf.get("host")),
        port=os.getenv("RAW_DB_PORT", raw_db_conf.get("port")),
        user=os.getenv("RAW_DB_USER", raw_db_conf.get("user")),
        password=os.getenv("RAW_DB_PASSWORD", raw_db_conf.get("password")),
        database=os.getenv("RAW_DB_NAME", raw_db_conf.get("database"))
    )
    
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM raw_readings;")
        row_count = cur.fetchone()[0]
    conn.close()
    
    return Output(
        value=row_count,
        metadata={
            "database_name": MetadataValue.text(raw_db_conf.get("database", "")),
            "table_name": MetadataValue.text("raw_readings"),
            "raw_record_count": MetadataValue.int(row_count)
        }
    )

@multi_asset(
    outs={
        "readings": AssetOut(group_name="analytics_warehouse"),
        "stations": AssetOut(group_name="analytics_warehouse"),
        "constituencies": AssetOut(group_name="analytics_warehouse"),
    },
    deps=[raw_readings]
)
def cleaned_readings(context: AssetExecutionContext):
    """
    Extracts raw readings in batches, applies data quality rules, checks boundary thresholds,
    calculates row checksums, and loads the validated records into the PostgreSQL OLAP serving layer.
    Outputs three database assets representing the loaded tables.
    """
    config = load_config()
    raw_db_conf = config.get("raw_db", {})
    olap_db_conf = config.get("olap_db", {})
    
    start_time = time.time()
    
    raw_conn = psycopg2.connect(
        host=os.getenv("RAW_DB_HOST", raw_db_conf.get("host")),
        port=os.getenv("RAW_DB_PORT", raw_db_conf.get("port")),
        user=os.getenv("RAW_DB_USER", raw_db_conf.get("user")),
        password=os.getenv("RAW_DB_PASSWORD", raw_db_conf.get("password")),
        database=os.getenv("RAW_DB_NAME", raw_db_conf.get("database"))
    )
    
    olap_conn = psycopg2.connect(
        host=os.getenv("OLAP_DB_HOST", olap_db_conf.get("host")),
        port=os.getenv("OLAP_DB_PORT", olap_db_conf.get("port")),
        user=os.getenv("OLAP_DB_USER", olap_db_conf.get("user")),
        password=os.getenv("OLAP_DB_PASSWORD", olap_db_conf.get("password")),
        database=os.getenv("OLAP_DB_NAME", olap_db_conf.get("database"))
    )
    
    try:
        # Recreate target OLAP serving schemas and seed static reference tables
        setup_olap_schema(olap_conn)
        seed_olap_static_tables(olap_conn, raw_conn)
        
        # Keyset pagination extraction and validation streaming loop
        chunk_size = config.get("etl", {}).get("chunk_size", 10000)
        batch_generator = extract_raw_data_in_batches(raw_conn, chunk_size=chunk_size)
        
        total_extracted = 0
        total_loaded = 0
        
        for raw_batch in batch_generator:
            total_extracted += len(raw_batch)
            cleaned_batch = transform_batch(raw_batch, config)
            if cleaned_batch:
                load_clean_batch_to_olap(olap_conn, cleaned_batch)
                total_loaded += len(cleaned_batch)
                
        elapsed = time.time() - start_time
        throughput = total_extracted / elapsed if elapsed > 0 else 0
        
        # Fetch actual counts of stations and constituencies loaded
        with olap_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM stations;")
            stations_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM constituencies;")
            constituencies_count = cur.fetchone()[0]

        yield Output(
            value=total_loaded,
            output_name="readings",
            metadata={
                "raw_extracted_rows": MetadataValue.int(total_extracted),
                "warehouse_loaded_rows": MetadataValue.int(total_loaded),
                "dropped_anomalies_count": MetadataValue.int(total_extracted - total_loaded),
                "pipeline_execution_seconds": MetadataValue.float(round(elapsed, 2)),
                "telemetry_throughput_rows_per_second": MetadataValue.float(round(throughput, 2))
            }
        )
        yield Output(
            value=stations_count,
            output_name="stations",
            metadata={
                "stations_count": MetadataValue.int(stations_count)
            }
        )
        yield Output(
            value=constituencies_count,
            output_name="constituencies",
            metadata={
                "constituencies_count": MetadataValue.int(constituencies_count)
            }
        )
    finally:
        raw_conn.close()
        olap_conn.close()

class CustomDagsterDbtTranslator(DagsterDbtTranslator):
    def get_group_name(self, dbt_resource_props: Mapping[str, Any]) -> str:
        return "analytics_warehouse"

    def get_asset_key(self, dbt_resource_props: Mapping[str, Any]) -> AssetKey:
        if dbt_resource_props["resource_type"] == "source":
            # Map dbt source tables ('readings', 'stations', 'constituencies') to their unique asset keys
            table_name = dbt_resource_props["name"]
            return AssetKey(table_name)
        return super().get_asset_key(dbt_resource_props)

dagster_dbt_translator = CustomDagsterDbtTranslator()

@dbt_assets(manifest=dbt_project.manifest_path, dagster_dbt_translator=dagster_dbt_translator)
def dbt_warehouse(context: AssetExecutionContext, dbt: DbtCliResource):
    """
    Executes the analytical transformations (staging, marts, DEFRA health indicators, averages)
    directly inside the PostgreSQL database using dbt.
    """
    yield from dbt.cli(["build"], context=context).stream()

@asset(deps=[dbt_warehouse], group_name="nosql_replica")
def mongodb_sample(context: AssetExecutionContext) -> Output[int]:
    """
    Fetches the latest data and static coordinates from PostgreSQL OLAP warehouse tables
    and denormalizes the observations into BSON document models inside MongoDB.
    """
    config = load_config()
    olap_db_conf = config.get("olap_db", {})
    nosql_db_conf = config.get("nosql_db", {})
    
    olap_conn = psycopg2.connect(
        host=os.getenv("OLAP_DB_HOST", olap_db_conf.get("host")),
        port=os.getenv("OLAP_DB_PORT", olap_db_conf.get("port")),
        user=os.getenv("OLAP_DB_USER", olap_db_conf.get("user")),
        password=os.getenv("OLAP_DB_PASSWORD", olap_db_conf.get("password")),
        database=os.getenv("OLAP_DB_NAME", olap_db_conf.get("database"))
    )
    
    columns = [
        "date_time", "site_id", "nox", "no2", "no", "pm10", "o3", "temp",
        "nvpm10", "vpm10", "nvpm2_5", "pm2_5", "vpm2_5",
        "co", "rh", "pressure", "so2", "row_checksum"
    ]
    col_str = ", ".join(columns)
    
    with olap_conn.cursor() as cur:
        # Fetch the first 1000 observations to build the serving sample
        cur.execute(f"SELECT {col_str} FROM readings LIMIT 1000;")
        rows = cur.fetchall()
        
    batch_data = []
    for r in rows:
        row_dict = dict(zip(columns, r))
        batch_data.append(row_dict)
        
    mongo_host = os.getenv("NOSQL_DB_HOST", nosql_db_conf.get("host"))
    mongo_port = os.getenv("NOSQL_DB_PORT", nosql_db_conf.get("port"))
    mongo_user = os.getenv("NOSQL_DB_USER", nosql_db_conf.get("user"))
    mongo_pw = os.getenv("NOSQL_DB_PASSWORD", nosql_db_conf.get("password"))
    mongo_db = os.getenv("NOSQL_DB_NAME", nosql_db_conf.get("database"))
    
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pw}@{mongo_host}:{mongo_port}/?authSource=admin"
    
    # Load and denormalize sample
    load_sample_to_mongodb(mongo_uri, mongo_db, "readings_sample", batch_data, olap_conn, limit=1000)
    olap_conn.close()
    
    return Output(
        value=len(batch_data),
        metadata={
            "mongodb_database": MetadataValue.text(mongo_db),
            "mongodb_collection": MetadataValue.text("readings_sample"),
            "documents_ingested_count": MetadataValue.int(len(batch_data))
        }
    )
