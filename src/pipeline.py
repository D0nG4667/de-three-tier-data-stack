import os
import sys
import time
import yaml
import logging
import psycopg2
from src.transform import transform_batch
from src.extract import extract_raw_data_in_batches
from src.load import setup_olap_schema, seed_olap_static_tables, load_clean_batch_to_olap, load_sample_to_mongodb
from src.validate import send_alert_notification

# Setup Logger
logger = logging.getLogger("pipeline")
logger.setLevel(logging.INFO)

# Console Handler
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

# File Handler
log_file = "/app/data/pipeline.log"
os.makedirs(os.path.dirname(log_file), exist_ok=True)
fh = logging.FileHandler(log_file)
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)
logger.addHandler(fh)

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/config.yaml")

def load_config() -> dict:
    """
    Loads and parses the pipeline configuration YAML file from the filesystem.
    
    Returns:
        dict: Parsed configurations dictionary (e.g. database credentials, ETL limits).
    """
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)

def run_pipeline() -> None:
    """
    Main ETL Orchestrator executing extraction, transformation, quality gates, and loading.
    
    The pipeline follows this exact sequence:
    1. Loads global settings from config.yaml.
    2. Opens transaction connections to staging raw and analytics OLAP databases.
    3. Truncates/creates target 3NF schema tables in Postgres OLAP.
    4. Extracts rows in cursor-based chunks from the raw database container.
    5. Cleanses and transforms records, keeping track of telemetry and metrics.
    6. Loads transformed fact records into the OLAP serving warehouse.
    7. Formats, denormalizes, and inserts a sample collection into the MongoDB NoSQL layer.
    8. Handles exceptions by triggering alerts and cleans up database connection handles.
    """
    logger.info("Initializing Bristol Air Quality ETL Pipeline...")
    start_time = time.time()
    
    # 1. Load Configurations
    try:
        config = load_config()
        logger.info("Configuration loaded successfully.")
    except Exception as e:
        logger.critical(f"Failed to load configurations: {e}")
        send_alert_notification(f"Pipeline failed at initialization. Unable to load config: {e}")
        sys.exit(1)

    # 2. Connect to Databases
    raw_db_conf = config.get("raw_db", {})
    olap_db_conf = config.get("olap_db", {})
    nosql_db_conf = config.get("nosql_db", {})

    logger.info("Connecting to raw and OLAP databases...")
    try:
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
        logger.info("Connections to database containers established successfully.")
    except Exception as e:
        logger.critical(f"Failed to establish database connections: {e}")
        send_alert_notification(f"Database connection failure: {e}")
        sys.exit(1)

    try:
        # 3. Setup Target Schema in OLAP Serving Layer
        setup_olap_schema(olap_conn)
        seed_olap_static_tables(olap_conn, raw_conn)

        # 4. Extract, Transform, and Load Readings in Batches
        chunk_size = config.get("etl", {}).get("chunk_size", 10000)
        batch_generator = extract_raw_data_in_batches(raw_conn, chunk_size=chunk_size)

        total_extracted = 0
        total_loaded = 0
        first_clean_batch_for_nosql = []

        for raw_batch in batch_generator:
            total_extracted += len(raw_batch)
            
            # Apply transformation (cropping, quality validations, row checksum)
            cleaned_batch = transform_batch(raw_batch, config)
            
            if cleaned_batch:
                # Load to target OLAP PostgreSQL tables
                load_clean_batch_to_olap(olap_conn, cleaned_batch)
                total_loaded += len(cleaned_batch)
                
                # Keep a sample of the first clean batch to seed MongoDB
                if not first_clean_batch_for_nosql:
                    first_clean_batch_for_nosql = cleaned_batch

        # 5. Seed NoSQL MongoDB Layer (Task 6)
        if first_clean_batch_for_nosql:
            mongo_host = os.getenv("NOSQL_DB_HOST", nosql_db_conf.get("host"))
            mongo_port = os.getenv("NOSQL_DB_PORT", nosql_db_conf.get("port"))
            mongo_user = os.getenv("NOSQL_DB_USER", nosql_db_conf.get("user"))
            mongo_pw = os.getenv("NOSQL_DB_PASSWORD", nosql_db_conf.get("password"))
            mongo_db = os.getenv("NOSQL_DB_NAME", nosql_db_conf.get("database"))
            
            # Construct MongoDB URI
            mongo_uri = f"mongodb://{mongo_user}:{mongo_pw}@{mongo_host}:{mongo_port}/?authSource=admin"
            load_sample_to_mongodb(mongo_uri, mongo_db, "readings_sample", first_clean_batch_for_nosql, olap_conn, limit=1000)

        # 6. Report Execution Summary Metrics
        elapsed = time.time() - start_time
        logger.info("==================================================")
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY!")
        logger.info(f"Total Rows Extracted: {total_extracted}")
        logger.info(f"Total Rows Loaded (OLAP): {total_loaded}")
        logger.info(f"Total Rows Dropped/Cleaned: {total_extracted - total_loaded}")
        logger.info(f"Execution Duration: {elapsed:.2f} seconds")
        logger.info("==================================================")

    except Exception as e:
        logger.error(f"Pipeline broke during execution: {e}", exc_info=True)
        send_alert_notification(f"Pipeline execution failed: {e}")
        sys.exit(1)
    finally:
        raw_conn.close()
        olap_conn.close()
        logger.info("Database connection resources closed.")

if __name__ == "__main__":
    run_pipeline()
