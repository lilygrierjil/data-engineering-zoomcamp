from pyspark.sql import types

schema = types.StructType([
    types.StructField('hvfhs_license_num', types.StringType(), True), 
    types.StructField('dispatching_base_num', types.StringType(), True), 
    types.StructField('originating_base_num', types.StringType(), True), 
    types.StructField('request_datetime', types.TimestampType(), True), 
    types.StructField('on_scene_datetime', types.TimestampType(), True), 
    types.StructField('pickup_datetime', types.TimestampType(), True), 
    types.StructField('dropoff_datetime', types.TimestampType(), True), 
    types.StructField('PULocationID', types.LongType(), True), 
    types.StructField('DOLocationID', types.LongType(), True), 
    types.StructField('trip_miles', types.DoubleType(), True), 
    types.StructField('trip_time', types.LongType(), True), 
    types.StructField('base_passenger_fare', types.DoubleType(), True), 
    types.StructField('tolls', types.DoubleType(), True), 
    types.StructField('bcf', types.DoubleType(), True), 
    types.StructField('sales_tax', types.DoubleType(), True), 
    types.StructField('congestion_surcharge', types.DoubleType(), True), 
    types.StructField('airport_fee', types.DoubleType(), True), 
    types.StructField('tips', types.DoubleType(), True), 
    types.StructField('driver_pay', types.DoubleType(), True), 
    types.StructField('shared_request_flag', types.StringType(), True), 
    types.StructField('shared_match_flag', types.StringType(), True), 
    types.StructField('access_a_ride_flag', types.StringType(), True), 
    types.StructField('wav_request_flag', types.StringType(), True), 
    types.StructField('wav_match_flag', types.StringType(), True)
])

types.StructType([types.StructField('hour', types.TimestampType(), True), 
            types.StructField('zone', types.LongType(), True), 
            types.StructField('revenue', types.DoubleType(), True), 
            types.StructField('count', types.LongType(), True)])


python more_spark_sql.py \
    --input_green=data/pq/green/2020/* \
    --input_yellow=data/pq/yellow/2020/* \
    --output=data/report-2020

URL="spark://de-zoomcamp.us-central1-c.c.enhanced-burner-375316.internal:7077"
spark-submit \
    --master "${URL}" \
    more_spark_sql.py \
    --input_green=data/pq/green/2021/* \
    --input_yellow=data/pq/yellow/2021/* \
    --output=data/report-2021


    --input_green=gs://de-zoomcamp-nytaxi/pq/green/2021/* \
    --input_yellow=gs://de-zoomcamp-nytaxi/pq/yellow/2021/* \
    --output=gs://de-zoomcamp-nytaxi/report-2021

gcloud dataproc jobs submit pyspark \
    gs://de-zoomcamp-nytaxi/code/more_spark_sql.py \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    -- \
    --input_green=gs://de-zoomcamp-nytaxi/pq/green/2020/* \
    --input_yellow=gs://de-zoomcamp-nytaxi/pq/yellow/2020/* \
    --output=gs://de-zoomcamp-nytaxi/report-2020
    
trips_data_all.reports-2020

gsutil cp spark_sql_bigquery.py gs://de-zoomcamp-nytaxi/code/spark_sql_bigquery.py

gcloud dataproc jobs submit pyspark \
    gs://de-zoomcamp-nytaxi/code/spark_sql_bigquery.py \
    --cluster=de-zoomcamp-cluster \
    --region=us-central1 \
    --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.23.2.jar \
    -- \
    --input_green=gs://de-zoomcamp-nytaxi/pq/green/2020/* \
    --input_yellow=gs://de-zoomcamp-nytaxi/pq/yellow/2020/* \
    --output=trips_data_all.reports-2020
