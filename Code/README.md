**Dependencies:**
	pyspark,
	numpy,
	pandas,
	joblib

***households.pkl*** file is used in BatchProcessing.py
This file contains a python dictionary containing values as ***house_id :[household_ids]***

I have provide the solution in two ways.

A) This is a streaming solution and it will require data pipeline setup.
   This is the final code which shall be used to get real-time alerts. This is divided into two files.
	1. StreamProcessing.py
	2. BatchProcessing.py

	How to run:
	1. Stream Processing
		spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 path/to/StreamProcessing.py
	2. Batch Processing
		This code is run in hourly batches and requires date-time as command line argument.
		Format of date-time is: dd-mm-yyyyTHH:MM:SS
		e.g. spark-submit path/to/BatchProcessing.py 03-09-2013T03:00:00
		This program wil append alerts to corresponding csv files
		We can create a shell script to run this job hourly

B) This section is to test streaming logic and get output for submission on AnalyticsVidhya.
I have attached this code as it may not be possible for you to check the results of the streaming code.
This part will run the same logic but as batch processing and will generate submission file.

	How to run:
	Create a folder 'submission'
	1. Stream Processing
		spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.1 path/to/StreamProcessingTest.py
	2. Batch Processing
		spark-submit path/to/BatchProcessingTest.py
	Alerts will be generated into 'submission' folder


