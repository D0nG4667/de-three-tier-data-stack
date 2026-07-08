# NoSQL Database Modeling and Implementation Report

## 1. Chosen NoSQL Database Model
For the Bristol Air Quality project, a **Document Store** model implemented via **MongoDB** was selected. 

### Justification
Air quality data is inherently semi-structured and time-series in nature. Each monitoring station records various environmental variables (temperature, relative humidity, pressure) alongside specific pollutant metrics (NOx, NO2, PM2.5). In a relational system, this requires heavy normalization, foreign key references, and multi-table joins to retrieve a single consolidated observation. A document store allows us to store each hourly reading as a self-contained document, encapsulating station metadata, weather conditions, and pollutant measurements in a single nested structure.

---

## 2. Denormalized Time-Series Schema Model
Below is the BSON/JSON schema structure applied to the MongoDB collection `readings_sample`.

```json
{
  "_id": {"$oid": "668ba42ff7b5a828e83c2710"},
  "date_time": "2019-10-01T08:00:00",
  "site_id": 188,
  "station": {
    "name": "AURN Bristol Centre",
    "latitude": 51.4572041156,
    "longitude": -2.58564914143,
    "constituency": {
      "name": "Bristol West",
      "mp_name": "Thangam Debbonaire"
    }
  },
  "pollutants": {
    "nox": 110.5,
    "no2": 45.2,
    "no": 65.3,
    "pm10": 18.2,
    "pm2_5": 11.1,
    "o3": 35.0,
    "co": 0.4,
    "so2": 1.2
  },
  "weather": {
    "temp": 12.5,
    "rh": 65.0,
    "pressure": 1015.0
  },
  "row_checksum": "abcf342938fd89a19234b3f81e83a6c1"
}
```

### Key Design Aspects:
1. **Nesting**: The station details, constituency mapping, and MP information are denormalized and nested inside the `station` sub-document. This guarantees that a single query retrieves all necessary context.
2. **Logical Grouping**: Pollutant observations are grouped under `pollutants`, and meteorological data is grouped under `weather` for structural cleanliness.
3. **Auditability**: The `row_checksum` is retained to ensure data lineage parity with the PostgreSQL OLAP warehouse.

---

## 3. Relational vs. NoSQL Architectural Contrast

| Dimension | Normalised Relational Model (PostgreSQL/MySQL) | De-normalised Document Model (MongoDB) |
|---|---|---|
| **Data Structure** | Tabular, strict schema, tables connected by Primary/Foreign Keys. | Hierarchical, schema-less, nested JSON/BSON documents. |
| **Storage Efficiency**| Highly efficient. Eliminates data redundancy (e.g. station name and coordinates are only stored once). | Redundant storage. Station details and constituency information are duplicated in every reading document. |
| **Query Performance** | Complex, multi-table joins (`JOIN readings JOIN stations JOIN constituencies`) consume CPU/RAM at scale. | Highly optimized for reads. Complete station context and readings are fetched in a single index lookup without joins. |
| **Write Performance** | Slower. Requires checking referential integrity constraints across tables. | Extremely fast. Document insertions do not check external foreign keys. |
| **Schema Evolution** | Rigorous. Requires `ALTER TABLE` DDL statements, which can lock databases during updates. | Seamless. New sensors can append new keys dynamically without impacting existing documents. |

---

## 4. Query Implementation and Outputs
To verify the NoSQL layer, the following query fetches the first 5 records from station `188` (AURN Bristol Centre) where the NO2 pollutant concentration exceeds `40.0 ㎍/m3` (representing a high concentration day):

### MongoDB Query (`query_nosql.js`)
```javascript
db.readings_sample.find(
    {
        "site_id": 188,
        "pollutants.no2": { $gt: 40.0 }
    },
    {
        "date_time": 1,
        "station.name": 1,
        "pollutants.no2": 1,
        "weather.temp": 1
    }
).sort({ "date_time": 1 }).limit(5).pretty();
```

### Simulated Output
```json
[
  {
    "_id": {"$oid": "668ba42ff7b5a828e83c2710"},
    "date_time": "2022-10-01T08:00:00",
    "station": { "name": "AURN Bristol Centre" },
    "pollutants": { "no2": 45.2 },
    "weather": { "temp": 12.5 }
  },
  {
    "_id": {"$oid": "668ba42ff7b5a828e83c2712"},
    "date_time": "2022-10-01T09:00:00",
    "station": { "name": "AURN Bristol Centre" },
    "pollutants": { "no2": 42.1 },
    "weather": { "temp": 13.1 }
  }
]
```

---

## 5. Architectural Recommendations & Use Cases
- **Relational Stack**: Recommended for general auditing, complex analytical reporting, and multi-dimensional aggregations (e.g. running dbt models to calculate city-wide averages).
- **NoSQL Stack**: Recommended for ingestion endpoints receiving high-velocity streaming data from active sensors, IoT telemetry feeds, and real-time dashboard applications that require sub-second UI updates without join bottlenecks.
