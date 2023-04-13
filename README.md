Objetive:
In this practice we will need to perform some data transformation with the public data set on London bicicles to obtain some insights on London cycling behavior.
There are two tables in the London dataset in BigQuery (https://bigquery.cloud.google.com/):
	1. bigquery-public-data:london_bicycles.cyle_hire: cycling hires trip data (1 entry contains 1 trip from one station towards another station)
	2. bigquery-public-data:london_bicycles.cycle_station: data about the cycling station (including latitude and longitude information)

To inspect these tables, you will need a GCP project (see instructions).

The goal in this coding challenge is to:
	* Step 1: Calculate the amount of rides traveled for all possible combination of start station and end station in the dataset.
	(for example, there are 3 rides from station_id 1 to station_id 2 and 10 rides from station_id 2 to station_id 1)
	* Step 2: Calculate the total distance covered for all the rides for possible combinations of start station and end station assuming the trip distance between the two stations can be simplified by straight line between the two station's locations 
	(following the example above, if the distance between station_id 1 and station_id 2 is 2 km then the distance covered from station_id 1 to station_id 2 is 6 km and 20 km vice versa)
	
The idea is thar you build a DataFlow (https://cloud.google.com/dataflow/docs/) pipeline to solve these questions and don't solve this directly with queries in BigQuery. Therefore the query should only extract the data and not apply any transformation on the data.

Evaluation

The output should be written as text files to Google Cloud Storage (https://cloud.google.com/storage/docs/') under a bucket name of choice and then a subfolder "output". 
	* Junior test: Each line in these files should be formated as a string:
	"start_station_id, stop_station_id, amount_of_rides" (following the example above, output should contain the lines "1,2,3" and "2,1,10"). Your data will be validated by validating the 100 combinations with the highest amount of rides.
	* Senior test: Each line in these files should be formated as a string:
	"start_station_id, stop_station_id, amount_of_rides, total_distance_between_stations" (following the example above, output should contain the lines "1,2,3" and "2,1,10"). Your data will be validated by validating the 100 combinations with the highest distances. We accept a deviation of 5% on the distance in kilometers. The distance can be submitted either in the form of integers or floats.

Instructions:

Some instructions to help you get started:
	1. Set-up
		1. Create a free GCP project: https://cloud.google.com/free/
		2. Install Google Cloud SDK: https://cloud.google.com/sdk/install
		3. Authenticate Google Cloud SDK via commend gcloud auth application-default login
	2. Create a Google Cloud Storage bucket: http://cloud.google.com/storage/docs/
	3. Create dataflow pipeline
		1. Make sure to enable the dataflow API from GCP console
		2. Make sure to set your pipeline options: https://cloud.google.com/dataflow/docs/guides/specifying-exec-params
		3. Make sure to set the <a href='https://beam.apache.org/documentation/sdks/python-pipeline-dependencies/'>python pipeline dependencies</a> in a setup.py file:
		(also make sure you have the same versions installed locally)
		
			#!/usr/bin/python
			from setuptools import find_packages
			from setuptools import setup
			
			setup(
				name='Coding-Challenge',
				version='1.0',
				install_requires=[
					'apache-beam[gcp]==2.13.0',
					'geopy==1.16.0',
				],
				packages=find_packages(exclude=['notebooks']),
				py_modules=['config'],
				include_package_data=True,
				description-'Coding Challenge'
			)
		4. Build the pipeline:
			https://beam.apache.org/documentation/programming-guide/	

