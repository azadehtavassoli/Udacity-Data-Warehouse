# Data Warehouse

Project Data Warehouse done by Azadeh Tavassol as part of the Udactiy Data Engineer Nanodegree.

## Project Summary
An implementation of a Data Warehouse leveraging AWS RedShift. This projects contains the ETL pipeline that extracts data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for Sparkify analytics team.

The data on S3 contains song and log information from a music store. This solution enables music stores to easily process loads of information efficiently.

## Purpose of this project
In this project data from different sources i.e., multiple S3 buckets are processed to enable efficient and easy analytics. Hence, the Sparkify thus benefits from the eased analysis of the run-time data of application.

## Project instructions
1. Setup a redshift cluster on AWS and insert the connection details in `dwh.cfg`.
2. Create the needed the database structure by executing `create_tables.py`.
3. Process the data from the configured S3 data sources by executing `etl.py`.

## Database schema
| Table | Description |
| ---- | ---- |
| staging_events | stating table for event data |
| staging_songs | staging table for song data |
| songplays | information how songs were played, e.g. when by which user in which session | 
| users | user-related information such as name, gender and level | 
| songs | song-related information containing name, artist, year and duration | 
| artists | artist name and location (geo-coords and textual location) | 
| time | time-related info for timestamps | 

## ETL pipeline
1. Load song and log data both from S3 buckets.
2. Stage the data.
3. Transform the data into the data schema described above.