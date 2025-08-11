--- Count of Records in table (338,293,677)
SELECT COUNT(*) from Samples."samples.dremio.com"."NYC-taxi-trips-iceberg";

--- running aggregations on taxi (cold run: 15sec with no reflections or cache)
SELECT AVG(passenger_count) as avg_passengers,
       AVG(fare_amount) as avg_fare,
       AVG(trip_distance_mi) as avg_distance
FROM   Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"

--- after reflections (different aggregations to avoid cache)
SELECT AVG(total_amount) as avg_total,
       AVG(tip_amount) as tip,
FROM   Samples."samples.dremio.com"."NYC-taxi-trips-iceberg"
