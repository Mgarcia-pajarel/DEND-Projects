### Overview

This Project is created to handle data originating in a music streaming startup, Sparkify. Data set is a set of files in JSON format stored in AWS S3 buckets and contains two parts:

* **s3://udacity-dend/song\_data**: static data about artists and songs

  Song-data example:
  `{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}`

* **s3://udacity-dend/log\_data**: event data of service usage e.g. who listened what song, when, where, and with which client

![Log-data example][image-1]

### Project Background

* Sparkify provides music streaming to end users. Data of song details and user activities is captured as JSON and CSV files.

* AWS Redshift is the selected data warehousing platform, enabling persistent data storage and ad-hoc queries.

* Apache Airflow is designated as the data pipeline solution that supports automation as well as monitoring of the ETL process.


### Redshift Setup (Amazon Web Services)
 
* Create Redshift IAM role with S3FullAccess.

* Create Redshift cluster.

* Attach Redshift IAM role to the cluster.

* create empty staging tables, dimension tables, and fact table as groundwork.


### Airflow Setup

* Start Airflow with `/opt/airflow/start.sh` in command line.

* Create connection to S3.

* Create connection to Redshift.


### Data Sources

* Log data: `s3://udacity-dend/log_data`

* Song data: `s3://udacity-dend/song_data`


### Final Tables

* Songplays Fact Table. Records in log data associated with song plays i.e. records with page `NextSong` [songplay\_id, start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent]

* Users Dimension Table. Users in the app [user\_id, first\_name, last\_name, gender, level]

* Songs Dimension Table. Songs in music database [song\_id, title, artist\_id, year, duration]

* Artists Dimension Table. Artists in music database [artist\_id, name, location, latitude, longitude]

* Time Dimension Table. Timestamps of records broken-down into specific units [start\_time, hour, day, week, month, year, weekday]

### Data Pipelines

![Log-data example][image-2]

### ETL Scripts

* `create_tables.sql` : Data Definition Language (DDL) for 2 staging tables, 4 dimension tables, and 1 fact table for the project.

* `airflow_dag.py` : DAG file for Apache Airflow. This generates the DAG with all necessary tasks to read the files from the S3 buckets, load data into staging tables and transform into a star schema which is stored in Redshift.

* `stage_redshift.py` : custom staging operator to load data from S3 to Redshift. Using Airflow's PostgreSQL & S3 hooks, data is read and copied to staging tables in Redshift.

* `load_fact.py` : custom operator to populate fact table in Redshift.

* `load_dimension.py` : custom operator to load dimension tables in Redshift.

* `data_quality.py` : custom operator to check data quality in all tables. 


[image-1]:	./DEND-Airflow-logdata.png
[image-2]:	./flow-dag.png

Last updated on July 24, 2019.