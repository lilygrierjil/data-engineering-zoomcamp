## Week 1 Homework: Lily's Solutions

In this homework we'll prepare the environment 
and practice with Docker and SQL


## Question 1. Knowing docker tags

Run the command to get information on Docker 

```docker --help```

Now run the command to get help on the "docker build" command

Which tag has the following text? - *Write the image ID to the file* 

- `--imageid string`
- `--iidfile string`
- `--idimage string`
- `--idfile string`

Ran `docker build --help`

## Question 2. Understanding docker first run 

Run docker with the python:3.9 image in an interactive mode and the entrypoint of bash.
Now check the python modules that are installed ( use pip list). 
How many python packages/modules are installed?

- 1
- 6
- 3
- 7

Ran `docker run -it python 3:9 bash`
followed by `pip list`

Found 3 packages.

# Prepare Postgres

Run Postgres and load data as shown in the videos
We'll use the green taxi trips from January 2019:

```wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz```

You will also need the dataset with zones:

```wget https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv```

Download this data and put it into Postgres (with jupyter notebooks or with a pipeline)



## Question 3. Count records 

How many taxi trips were totally made on January 15?

Tip: started and finished on 2019-01-15. 

Remember that `lpep_pickup_datetime` and `lpep_dropoff_datetime` columns are in the format timestamp (date and hour+min+sec) and not in date.

- 20689
- 20530
- 17630
- 21090

First I booted up postgres and pg-admin using `docker-compose up`


I modified ingest_data.py in this folder to handle both tables.

Then, ran `export URL=https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz`

Then, ran the following to upload green tripdata locally.
```
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=green_taxi_trips \
  --url=${URL}
```

Then, changed the URL value by running `export URL=https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv`

And then ran
```
python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=zones \
  --url=${URL}
```

I ran the following query:
```
SELECT COUNT(1)
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS date) = '2019-01-15'
AND CAST(lpep_dropoff_datetime AS date) = '2019-01-15';
```
to get 20530.

## Question 4. Largest trip for each day

Which was the day with the largest trip distance
Use the pick up time for your calculations.

- 2019-01-18
- 2019-01-28
- 2019-01-15
- 2019-01-10


I used the following query:

```
SELECT 
	lpep_pickup_datetime
FROM green_taxi_trips
ORDER BY trip_distance DESC
LIMIT 1;
```

to come up with 2019-01-15.



## Question 5. The number of passengers

In 2019-01-01 how many trips had 2 and 3 passengers?
 
- 2: 1282 ; 3: 266
- 2: 1532 ; 3: 126
- 2: 1282 ; 3: 254
- 2: 1282 ; 3: 274


Query for 2 passengers:
```
SELECT COUNT(1)
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS date) = '2019-01-01'
AND passenger_count = 2;
```

Query for 3 passengers:
```
SELECT COUNT(1)
FROM green_taxi_trips
WHERE CAST(lpep_pickup_datetime AS date) = '2019-01-01'
AND passenger_count = 3;
```

This returned 1282 trips with 2 passengers and 254 trips with 3 passengers.

## Question 6. Largest tip

For the passengers picked up in the Astoria Zone which was the drop off zone that had the largest tip?
We want the name of the zone, not the id.

Note: it's not a typo, it's `tip` , not `trip`

- Central Park
- Jamaica
- South Ozone Park
- Long Island City/Queens Plaza

I used the following query:
```
SELECT "Zone"
FROM green_taxi_trips t
JOIN zones z
ON t."DOLocationID" = z."LocationID"
WHERE t."PULocationID" = 
(SELECT "LocationID"
FROM zones
WHERE "Zone" = 'Astoria')
ORDER BY tip_amount DESC
LIMIT 1;
```
and got Long Island City/Queens Plaza.


