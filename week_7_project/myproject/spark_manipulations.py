import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import types



mpd_schema = types.StructType([
    types.StructField("crime_id", types.StringType(), True),
    types.StructField("offense_date", types.StringType(), True),
    types.StructField("agency_crimetype_id", types.StringType(), True),
    types.StructField("city", types.StringType(), True),
    types.StructField("state", types.StringType(), True),
    types.StructField("masked_address", types.StringType(), True),
    types.StructField("category", types.StringType(), True),
    types.StructField("coord1", types.DoubleType(), True),
    types.StructField("coord2", types.DoubleType(), True),
    types.StructField("location", types.MapType(types.DoubleType(), types.DoubleType()), False),
    types.StructField("offense_date_datetime", types.TimestampType(), True),
    types.StructField("offense_date_day", types.DateType(), True)
])

