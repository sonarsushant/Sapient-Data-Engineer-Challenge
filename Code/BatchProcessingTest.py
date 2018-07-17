### Use this code to check output of the data pipeline by doing batch processing on whole data.
###Please extract the test_data folder and create a folder named 'submission'
###You need to run StreamProcessingTest.py first. It will create parquet file required for this program

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.functions import abs as abs_
from pyspark.sql.window import Window,WindowSpec
import pandas as pd
import numpy as np
from datetime import datetime as dt
import joblib
import sys

#Created some UDFs
#This will return alert if standard_deviation>1
def get_result(x):
    if x>1:
        return 1
    else:
        return 0

#This function returns string in the formation house_household_date_hour
def get_id(date,house_id,household_id,hour):
    return (str(house_id)+'_'+str(household_id)+'_'+date.strftime('%d-%m-%Y')+'_'+str(hour))

def median(values_list):
    med = np.median(values_list)
    return float(med)

median_udf = udf(median, FloatType())
result_udf=udf(f=get_result,returnType=IntegerType())
id_udf=udf(f=get_id)

fromdate=dt.strptime('31-08-2013T22:00:00','%d-%m-%YT%H:%M:%S')
todate=dt.strptime('30-09-2013T22:00:00','%d-%m-%YT%H:%M:%S')
date=todate.date()
dateRange=pd.date_range(start=fromdate,end=todate,freq='H')

#'households.pkl' file contains dictionary {house_id:array of household_ids}
#Read this file and create a reference dataframe with date_time,house,house_id combination
#Use this dataframe to detect missing data
households=joblib.load('housleholds.pkl')
house=[]
household=[]
for i,j in households.items():
    house.extend([i]*len(j))
    household.extend(j)

df=pd.DataFrame(index=range(len(house)))
df['house_id']=house
df['household_id']=household
df['date_time']=dateRange[0]

for d in dateRange[1:]:
    df1=pd.DataFrame(index=range(len(house)))
    df1['house_id']=house
    df1['household_id']=household
    df1['date_time']=d
    df=pd.concat([df,df1],ignore_index=True)

#Create spark session
spark=SparkSession.builder.config('spark.executor.instances',2).config('spark.executor.memory','3g').\
        getOrCreate()

#Convert reference dataframe to spark dataframe
spark_df=spark.createDataFrame(df)

#read data from parquet file
grouped=spark.read.parquet('grouped_parquet/')

#Join reference dataframe and original dataframe
joined=spark_df.join(grouped,on=['house_id','household_id','date_time'],how='outer')

#Create 'hour' column
joined=joined.withColumn('hour',hour('date_time'))

# Impute nulls by median
aggWindow=Window.partitionBy('house_id','household_id','hour').orderBy('date_time').rowsBetween(float('-inf'),0)
joined = joined.withColumn('value',when(isnull('value'),median_udf(collect_list(col('value')).over(aggWindow))).otherwise(col('value')))


###Alert_Type_1###

#Create a window partitioned by 'house_id','household_id','hour'
#Use it to calculate historical mean and standard deviation
#As standard deviation for 1st values in each partition is NaN, fill corresponding results by 0
w=Window.partitionBy('house_id','household_id','hour').orderBy('date_time').rowsBetween(float('-inf'),0)
joined_1=joined.withColumn('result',(col('value')-avg('value').over(w))/stddev('value').over(w))
joined_1=joined_1.fillna({'result':0})
joined_1=joined_1.select('house_id','household_id','date_time','hour','result')


joined_1.filter("date_time = cast('{}' as TIMESTAMP)".format(d)).count()

#Apply UDFs to get 'id' and 'alert' column
joined_1=joined_1.withColumn('alert',result_udf('result')).withColumn('id',id_udf('date_time',
                                                                              'house_id','household_id','hour'))
joined_1=joined_1.select('id','alert')

a1=joined_1.toPandas()
a1.to_csv('alert_type_1.csv',index=False)


###Alert_Type_2###
filtered=joined.filter("date_time = cast('{}' as TIMESTAMP)".format(todate))

#Calculate mean and std_dev of all households for that hour on that day
grouped_2=filtered.groupBy('date_time').agg(avg('value').alias('mean'),stddev('value').alias('stddev'))
joined_2=filtered.join(grouped_2,on='date_time',how='left')

#Calculate 'result' which shows how many std_dev current rating is
joined_2=joined_2.withColumn('result',(col('value')-col('mean'))/col('stddev'))

#As standard deviation for 1st value is NaN, fill corresponding results by 0
joined_2=joined_2.fillna({'result':0})

#Apply UDFs to get 'id' and 'alert' column
joined_2=joined_2.withColumn('alert',result_udf('result')).withColumn('id',id_udf('date_time',                                                                             'house_id','household_id','hour'))

joined_2=joined_2.select('id','alert')
a2=joined_2.toPandas()
a2.to_csv('alert_type_2.csv',index=False)


##Create submission file for AnalyticsVidhya
test1=pd.read_csv('test_sZn4Axl/alert_type_1.csv')
predict1=pd.read_csv('solution/alert_type_1.csv')

test2=pd.read_csv('test_sZn4Axl/alert_type_2.csv')
predict2=pd.read_csv('solution/alert_type_2.csv')

test1=pd.merge(test1,predict1,how='left',on='id')
#test1['alert'].fillna(0,inplace=True)
test1['alert']=test1['alert'].apply(int)
test1.to_csv('submission/alert_type_1.csv',index=False)

test2=pd.merge(test2,predict2,how='left',on='id')
#test2['alert'].fillna(0,inplace=True)
test2['alert']=test2['alert'].apply(int)
test2.to_csv('submission/alert_type_2.csv',index=False)