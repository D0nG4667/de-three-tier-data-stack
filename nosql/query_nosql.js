// UFCFLR-15-M Data Management Fundamentals
// Modelling & Mapping Bristol Air Quality Data
// Task 6: NoSQL MongoDB Query Example

// Select the database
db = db.getSiblingDB("bristol_nosql");

// Example Query: Find all hourly observation records for station 188 (AURN Bristol Centre)
// where Nitrogen Dioxide (NO2) concentration exceeds 40 mcg/m3 (typical UK annual mean objective)
// and sort them chronologically (oldest first).
db.readings_sample.find(
    {
        "site_id": 188,
        "pollutants.no2": { $gt: 40.0 }
    },
    {
        "date_time": 1,
        "station.name": 1,
        "pollutants.no2": 1,
        "weather.temperature": 1
    }
).sort({ "date_time": 1 }).limit(5).pretty();
