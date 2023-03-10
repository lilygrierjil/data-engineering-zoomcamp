import faust
from taxi_rides import TaxiRide


app = faust.App('datatalksclub.stream.v2', broker='kafka://localhost:9092')
topic = app.topic('datatalkclub.green_taxi_ride.json', value_type=TaxiRide)

pu_loc_count = app.Table('pickup_location_count', default=int)


@app.agent(topic)
async def process(stream):
    async for event in stream.group_by(TaxiRide.PULocationID):
        pu_loc_count[event.PULocationID] += 1
        print(pu_loc_count)

if __name__ == '__main__':
    app.main()