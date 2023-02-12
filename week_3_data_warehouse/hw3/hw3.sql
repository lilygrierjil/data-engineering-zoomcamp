-- Create the external table
CREATE OR REPLACE EXTERNAL TABLE `buoyant-song-375701.fhv.external_fhv`
OPTIONS (
  format = 'CSV',
  uris = ['gs://prefect-de-zoomcamp-lily/data/fhv_tripdata_2019-*.csv.gz']
);

-- Create a regular table from the external table
CREATE OR REPLACE TABLE fhv.materialized_fhv
AS SELECT * FROM fhv.external_fhv;

-- Question 1
SELECT COUNT(*)
FROM fhv.external_fhv;

-- Question 2
SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv.external_fhv; -- O B

SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv.materialized_fhv; -- 317.94 MB

-- Question 3
SELECT COUNT(*)
FROM fhv.external_fhv
WHERE PUlocationID IS NULL
AND DOlocationID IS NULL;

-- Question 5
CREATE OR REPLACE TABLE fhv.fhv_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM fhv.external_fhv;

SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv.materialized_fhv
WHERE DATE(pickup_datetime) BETWEEN "2019-03-01" AND "2019-03-31"; -- 647.87 MB

SELECT COUNT(DISTINCT affiliated_base_number)
FROM fhv.fhv_partitioned_clustered
WHERE DATE(pickup_datetime) BETWEEN "2019-03-01" AND "2019-03-31"; -- 23.05 MB

-- Question 8
-- Create the external table
CREATE OR REPLACE EXTERNAL TABLE `buoyant-song-375701.fhv.external_fhv_parquet`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://prefect-de-zoomcamp-lily/data/fhv_tripdata_2019-*.parquet']
);

-- Create a regular table from the external table
CREATE OR REPLACE TABLE fhv.materialized_fhv_parquet
AS SELECT * FROM fhv.external_fhv_parquet;

SELECT COUNT(*)
FROM fhv.external_fhv_parquet;