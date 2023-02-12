CREATE OR REPLACE EXTERNAL TABLE `trips_data_all.fhv_ext`
OPTIONS (
  format = 'parquet',
  uris = ['gs://dtc_data_lake_tidy-bliss-376908/data/fhv_2019-*.parquet']
);

SELECT COUNT(*) from trips_data_all.fhv

CREATE OR REPLACE TABLE `trips_data_all.fhv`
AS SELECT * FROM `trips_data_all.fhv_ext`


SELECT DISTINCT affiliated_base_number 
FROM `trips_data_all.fhv_ext`

SELECT DISTINCT affiliated_base_number 
FROM `trips_data_all.fhv`

SELECT COUNT (*) 
FROM `trips_data_all.fhv`
WHERE PUlocationID is null and DOlocationID is null

SELECT DISTINCT affiliated_base_number 
FROM `trips_data_all.fhv`
WHERE pickup_datetime >= TIMESTAMP("2019/03/01") and pickup_datetime <= TIMESTAMP("2019/03/31")

CREATE OR REPLACE TABLE `trips_data_all.fhv_partitioned_clustered`
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number
AS SELECT * FROM `trips_data_all.fhv_ext`

SELECT DISTINCT affiliated_base_number 
FROM `trips_data_all.fhv_partitioned_clustered`
WHERE pickup_datetime >= TIMESTAMP("2019-03-01") and pickup_datetime <= TIMESTAMP("2019-03-31")

