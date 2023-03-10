import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('data/green_tripdata_2019-01.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for i, row in enumerate(csvreader):
    key = {"PULocationID": str(row[5])}
    value = {"vendorId": str(row[0]), "PULocationID": str(row[5]), "DOLocationID": str(row[6])}
    producer.send('datatalkclub.green_taxi_ride.json', value=value, key=key)
    #print(value)
    sleep(1)
    # if i > 10:
    #     break


file = open('data/fhv_tripdata_2019-01.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for i, row in enumerate(csvreader):
    key = {"PULocationID": str(row[3])}
    value = {"vendorId": str(row[0]), "PULocationID": str(row[3]), "DOLocationID": str(row[4])}
    producer.send('datatalkclub.green_taxi_ride.json', value=value, key=key)
    #print(value)
    sleep(1)
    # if i > 10:
    #     break