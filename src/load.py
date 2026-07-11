import logging
from typing import Any, List, Dict
from pymongo import MongoClient

logger = logging.getLogger("pipeline")

def setup_olap_schema(conn: Any) -> None:
    """
    Initializes target tables and partitioning indexes in the OLAP serving database.
    
    This function drops existing tables to prevent schema collision and creates a normalized
    3NF target schema (constituencies, stations, readings). It also creates composite and 
    compound indexes to accelerate multi-dimensional analytical query performance.
    
    Parameters:
        conn (Any): Active psycopg2 connection object to the OLAP serving database.
    """
    with conn.cursor() as cur:
        logger.info("Initializing target tables in OLAP database...")
        
        # Drop existing tables if necessary
        cur.execute("""
            DROP TABLE IF EXISTS readings CASCADE;
            DROP TABLE IF EXISTS stations CASCADE;
            DROP TABLE IF EXISTS constituencies CASCADE;

            CREATE TABLE constituencies (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                mp_name VARCHAR(100) NOT NULL
            );

            CREATE TABLE stations (
                site_id INT PRIMARY KEY,
                name VARCHAR(150) NOT NULL,
                latitude DECIMAL(12,9) NOT NULL,
                longitude DECIMAL(12,9) NOT NULL,
                constituency_id INT REFERENCES constituencies(id),
                date_start TIMESTAMP,
                date_end TIMESTAMP,
                is_current BOOLEAN,
                instrument_type VARCHAR(100)
            );

            CREATE TABLE readings (
                id SERIAL PRIMARY KEY,
                date_time TIMESTAMP NOT NULL,
                site_id INT REFERENCES stations(site_id),
                nox REAL,
                no2 REAL,
                no REAL,
                pm10 REAL,
                o3 REAL,
                temperature REAL,
                nvpm10 REAL,
                vpm10 REAL,
                nvpm2_5 REAL,
                pm2_5 REAL,
                vpm2_5 REAL,
                co REAL,
                rh REAL,
                air_pressure REAL,
                so2 REAL,
                row_checksum VARCHAR(32) NOT NULL
            );

            -- Add partitioning indices/performance optimizations
            CREATE INDEX idx_readings_date_time ON readings(date_time);
            CREATE INDEX idx_readings_site_id ON readings(site_id);
            CREATE INDEX idx_readings_compound ON readings(site_id, date_time);
        """)
        conn.commit()
        logger.info("OLAP target schemas created successfully.")

def seed_olap_static_tables(conn: Any, raw_conn: Any) -> None:
    """
    Replicates static dimension tables from the raw database to the OLAP serving database.
    
    Queries the raw staging database to extract constituencies and stations, and loads them
    into the OLAP serving warehouse to preserve referential integrity before fact records are streamed.
    
    Parameters:
        conn (Any): Active psycopg2 connection object to the OLAP serving database.
        raw_conn (Any): Active psycopg2 connection object to the raw staging database.
    """
    with raw_conn.cursor() as raw_cur:
        # Fetch constituencies
        raw_cur.execute("SELECT id, name, mp_name FROM raw_constituencies;")
        consts = raw_cur.fetchall()
        
        # Fetch stations
        raw_cur.execute(
            """
            SELECT site_id, name, latitude, longitude, constituency_id,
                   date_start, date_end, is_current, instrument_type 
            FROM raw_stations;
            """
        )
        stats = raw_cur.fetchall()

    with conn.cursor() as cur:
        logger.info("Seeding OLAP constituencies and stations...")
        for const in consts:
            cur.execute(
                "INSERT INTO constituencies (id, name, mp_name) VALUES (%s, %s, %s) ON CONFLICT (id) DO NOTHING;",
                const
            )
        for stat in stats:
            cur.execute(
                """
                INSERT INTO stations (
                    site_id, name, latitude, longitude, constituency_id,
                    date_start, date_end, is_current, instrument_type
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (site_id) DO NOTHING;
                """,
                stat
            )
        conn.commit()
        logger.info("Static tables seeded in OLAP database.")

def load_clean_batch_to_olap(conn: Any, batch: List[Dict[str, Any]]) -> None:
    """
    Inserts a cleaned batch of transformed readings into the OLAP readings table.
    
    Leverages psycopg2's executemany bulk execution to perform batch database writes. 
    It maps pandas-cleansed dictionaries to SQL insert tuples.
    
    Parameters:
        conn (Any): Active psycopg2 connection object to the OLAP serving database.
        batch (List[Dict[str, Any]]): List of cleaned reading dictionaries to load.
    """
    if not batch:
        return
        
    insert_query = """
        INSERT INTO readings (
            date_time, site_id, nox, no2, no, pm10, o3, temperature,
            nvpm10, vpm10, nvpm2_5, pm2_5, vpm2_5,
            co, rh, air_pressure, so2, row_checksum
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        );
    """
    
    records = []
    for row in batch:
        records.append((
            row["date_time"], row["site_id"], row["nox"], row["no2"], row["no"],
            row["pm10"], row["o3"], row["temperature"],
            row["nvpm10"], row["vpm10"], row["nvpm2_5"], row["pm2_5"], row["vpm2_5"],
            row["co"], row["rh"], row["air_pressure"], row["so2"], row["row_checksum"]
        ))
        
    with conn.cursor() as cur:
        cur.executemany(insert_query, records)
        conn.commit()
    logger.info(f"Loaded batch of {len(batch)} records to OLAP database.")

def load_sample_to_mongodb(
    mongo_uri: str,
    db_name: str,
    collection_name: str,
    batch: List[Dict[str, Any]],
    olap_conn: Any,
    limit: int = 1000
) -> None:
    """
    Formats, denormalizes, and writes a representative sample of readings to MongoDB.
    
    Instead of using static mock maps in Python memory, this function queries the live 
    PostgreSQL OLAP database directly to resolve station coordinates and constituency details. 
    It denormalizes the data into nested BSON document models optimized for sub-second read latency.
    
    Parameters:
        mongo_uri (str): Connection URI for the MongoDB instance.
        db_name (str): Name of the target MongoDB database.
        collection_name (str): Target document collection name.
        batch (List[Dict[str, Any]]): List of transformed reading dictionaries.
        olap_conn (Any): Active psycopg2 connection object to the OLAP database.
        limit (int): Maximum number of records to ingest into the MongoDB sample.
    """
    if not batch:
        return
        
    try:
        # Establish MongoDB connection
        client = MongoClient(mongo_uri)
        db = client[db_name]
        collection = db[collection_name]
        
        # Clear existing entries in sample collection to allow clean re-runs
        collection.delete_many({})
        
        # Stations data cache to build denormalized profiles by querying PostgreSQL directly
        station_map = {}
        with olap_conn.cursor() as cur:
            cur.execute("""
                SELECT s.site_id, s.name, s.latitude, s.longitude, c.name, c.mp_name
                FROM stations s
                LEFT JOIN constituencies c ON s.constituency_id = c.id;
            """)
            for row in cur.fetchall():
                site_id, name, lat, lon, const_name, mp_name = row
                station_map[site_id] = {
                    "name": name,
                    "latitude": float(lat) if lat is not None else None,
                    "longitude": float(lon) if lon is not None else None,
                    "constituency": {
                        "name": const_name,
                        "mp_name": mp_name
                    }
                }
            
        mongo_docs = []
        # Sample only the first few records up to limit
        for row in batch[:limit]:
            site_id = row.get("site_id")
            station_profile = station_map.get(site_id, {})
            
            doc = {
                "date_time": row["date_time"].isoformat() if hasattr(row["date_time"], "isoformat") else str(row["date_time"]),
                "site_id": site_id,
                "station": station_profile,
                "pollutants": {
                    "nox": float(row["nox"]) if row.get("nox") is not None else None,
                    "no2": float(row["no2"]) if row.get("no2") is not None else None,
                    "no": float(row["no"]) if row.get("no") is not None else None,
                    "pm10": float(row["pm10"]) if row.get("pm10") is not None else None,
                    "pm2_5": float(row["pm2_5"]) if row.get("pm2_5") is not None else None,
                    "o3": float(row["o3"]) if row.get("o3") is not None else None,
                    "co": float(row["co"]) if row.get("co") is not None else None,
                    "so2": float(row["so2"]) if row.get("so2") is not None else None
                },
                "weather": {
                    "temperature": float(row["temperature"]) if row.get("temperature") is not None else None,
                    "rh": float(row["rh"]) if row.get("rh") is not None else None,
                    "air_pressure": float(row["air_pressure"]) if row.get("air_pressure") is not None else None
                },
                "row_checksum": row.get("row_checksum")
            }
            mongo_docs.append(doc)
            
        if mongo_docs:
            collection.insert_many(mongo_docs)
            logger.info(f"Loaded {len(mongo_docs)} denormalized time-series documents to MongoDB collection '{collection_name}'")
            
        client.close()
    except Exception as e:
        logger.error(f"Failed to load data to MongoDB: {e}")
