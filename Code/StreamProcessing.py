from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window,WindowSpec
import pandas as pd
from dateutil import parser, tz


#Create Spark Session
spark=SparkSession.builder.getOrCreate()

#Create Schema for data
schema=StructType([StructField('house_id',IntegerType()),
             StructField('household_id',IntegerType()),
             StructField('timestamp',StringType()),
             StructField('value',FloatType())
                ])

#Receive stream from Kafka topc 'NiFiToSpark'
data = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:6667") \
  .option("subscribe", "NiFiToSpark") \
  .load()

#Stream received from kafka is in string format. Split that string into four columns
data=data.select(col('key').cast('string'),col('value').cast('string'))
data=data.withColumn('house_id',split('value',',')[0].cast(IntegerType())).\
    withColumn('household_id',split('value',',')[1].cast(IntegerType())).\
    withColumn('timestamp',split('value',',')[2].cast(StringType())).\
    withColumn('value',split('value',',')[3].cast(IntegerType()))

#Drop duplicates from data
data=data.dropDuplicates()

#Create 'date_time' column from 'timestamp' as per GMT timezone.
#As spark by default convert timestamp to date_time using local timezone, I converted it to UTC/GMT date_time explicitely.
#'date_time' is combination of date and time e.g. 2013-09-01 03:00:00
data=data.withColumn('date_time',to_utc_timestamp(from_unixtime('timestamp'),'GMT+05:30').cast(TimestampType()))

#Group data by window of 1 hour, house_id and household_id
#As local time is 5hr30min ahead of utc, startTime of 30min is passed to window.
#Otherwise aggregate window will be 22:30-23:30, etc.
#Watermarking is must for streaming datasets.

grouped=data.withWatermark("date_time","1 hour").groupBy(window(col('date_time'),"1 hour",startTime="30 minutes"),'house_id','household_id').\
        agg(mean(col('value')).alias("value"))
    
grouped=grouped.select(grouped.window.start.alias("date_time"), grouped.window.end.alias("End"),
                       "house_id", "household_id","value")

#Create 'date' column which is required for partitioning
grouped=grouped.withColumn('date',to_date('date_time',format='yyyy-MM-dd'))

#Save aggegates to a parquet file with 'append' mode and partitioned by 'date'
grouped.writeStream.start(path='grouped_parquet/',partitionBy='date',                  mode='append',checkpointLocation='checkpoint/')

