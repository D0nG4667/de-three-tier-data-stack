-- UFCFLR-15-M Data Management Fundamentals
-- Modelling & Mapping Bristol Air Quality Data
-- Task 5(ii): Mean PM2.5 and VPM2.5 values at or near 08:00 hours for the year 2019 by station

SELECT 
    s.name AS station_name,
    AVG(CASE WHEN r.pm2_5 = 'NaN'::real THEN NULL ELSE r.pm2_5 END) AS mean_pm2_5,
    AVG(CASE WHEN r.vpm2_5 = 'NaN'::real THEN NULL ELSE r.vpm2_5 END) AS mean_vpm2_5
FROM readings r
JOIN stations s ON r.site_id = s.site_id
WHERE r.date_time >= '2019-01-01 00:00:00'
  AND r.date_time <= '2019-12-31 23:00:00'
  -- Extracting hour 8 represents observations taken at 08:00 hours
  AND EXTRACT(HOUR FROM r.date_time) = 8
GROUP BY s.name
ORDER BY s.name;
