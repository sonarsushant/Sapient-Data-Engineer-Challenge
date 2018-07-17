###This program tests my logic of StreamProcessing.py by doing batch operation on training data file
###This will also create the parquet file required to test batch processing

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

data=spark.read.csv('household/',schema=schema,header=True)

#Drop duplicates from data
data=data.dropDuplicates()

#Create 'date_time' column from 'timestamp' as per local timezone.
#'date_time' is combination of date and time e.g. 2013-09-01 03:00:00
data1=data.withColumn('date_time',to_utc_timestamp(from_unixtime('timestamp'),'GMT+05:30').cast(TimestampType()))


#Group data by window of 1 hour, house_id and household_id
#As local time is 5hr30min ahead of utc, startTime of 30min is passed to window.
#Otherwise aggregate window will be 3:30-4:30, etc.
#Watermarking is must for streaming datasets.

grouped=data1.withWatermark("date_time","1 hour").groupBy(window(col('date_time'),"1 hour"),'house_id','household_id').\
        agg(mean(col('value')).alias("value"))
    
grouped=grouped.select(grouped.window.start.alias("date_time"), grouped.window.end.alias("End"),
                       "house_id", "household_id","value")

#Create 'date' column which is required for partitioning
grouped=grouped.withColumn('date',to_date('date_time',format='yyyy-MM-dd'))

#Save aggegates to a parquet file with 'append' mode and partitioned by 'date'
grouped.write.parquet('grouped_parquet',mode='append',partitionBy='date')

