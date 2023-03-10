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
for row in csvreader:
    key = {"vendorId": str(row[0])}
    value = {"vendorId": str(row[0]), "PULocationID": str(row[5]), "DOLocationID": str(row[6])}
    producer.send('datatalkclub.green_taxi_ride.json', value=value, key=key)
    print(value)
    sleep(1)


file = open('data/fhv_tripdata_2019-01.csv')

csvreader = csv.reader(file)
header = next(csvreader)
for row in csvreader:
    key = {"vendorId": str(row[0])}
    value = {"vendorId": str(row[0]), "PULocationID": str(row[3]), "DOLocationID": str(row[4])}
    producer.send('datatalkclub.fhv.json', value=value, key=key)
    print(value)
    sleep(1)