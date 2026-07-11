import os
import sys
import yaml
import psycopg2
from pymongo import MongoClient

# Add parent path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.load import load_sample_to_mongodb

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config/config.yaml")

def main():
    print("Executing standalone MongoDB NoSQL Seeder...")
    
    # Load configuration
    try:
        with open(CONFIG_PATH, "r") as f:
            config = yaml.safe_load(f)
    except Exception as e:
        print(f"Error loading configuration: {e}")
        sys.exit(1)

    olap_db_conf = config.get("olap_db", {})
    nosql_db_conf = config.get("nosql_db", {})

    # Connect to db-olap
    print("Connecting to OLAP PostgreSQL Database...")
    try:
        conn = psycopg2.connect(
            host=os.getenv("OLAP_DB_HOST", olap_db_conf.get("host")),
            port=os.getenv("OLAP_DB_PORT", olap_db_conf.get("port")),
            user=os.getenv("OLAP_DB_USER", olap_db_conf.get("user")),
            password=os.getenv("OLAP_DB_PASSWORD", olap_db_conf.get("password")),
            database=os.getenv("OLAP_DB_NAME", olap_db_conf.get("database"))
        )
    except Exception as e:
        print(f"Database connection failure: {e}")
        sys.exit(1)

    # Fetch a sample of readings from OLAP
    columns = [
        "date_time", "site_id", "nox", "no2", "no", "pm10", "o3", "temperature",
        "nvpm10", "vpm10", "nvpm2_5", "pm2_5", "vpm2_5",
        "co", "rh", "air_pressure", "so2", "row_checksum"
    ]
    col_str = ", ".join(columns)
    
    print("Retrieving a sample of 1000 observations from OLAP readings...")
    with conn.cursor() as cur:
        cur.execute(f"SELECT {col_str} FROM readings LIMIT 1000;")
        rows = cur.fetchall()

    batch_data = []
    for r in rows:
        row_dict = dict(zip(columns, r))
        batch_data.append(row_dict)

    if not batch_data:
        print("No readings found in OLAP. Please run the ETL pipeline first to seed the data warehouse.")
        conn.close()
        sys.exit(1)

    # MongoDB connection details
    mongo_host = os.getenv("NOSQL_DB_HOST", nosql_db_conf.get("host"))
    mongo_port = os.getenv("NOSQL_DB_PORT", nosql_db_conf.get("port"))
    mongo_user = os.getenv("NOSQL_DB_USER", nosql_db_conf.get("user"))
    mongo_pw = os.getenv("NOSQL_DB_PASSWORD", nosql_db_conf.get("password"))
    mongo_db = os.getenv("NOSQL_DB_NAME", nosql_db_conf.get("database"))
    
    mongo_uri = f"mongodb://{mongo_user}:{mongo_pw}@{mongo_host}:{mongo_port}/?authSource=admin"

    # Seed MongoDB sample by passing PostgreSQL connection dynamically
    load_sample_to_mongodb(mongo_uri, mongo_db, "readings_sample", batch_data, conn, limit=1000)
    
    # Close PostgreSQL connection after MongoDB loading is completed
    conn.close()
    print("Standalone NoSQL seeding script completed successfully.")

if __name__ == "__main__":
    main()
