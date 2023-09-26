# Data Lake 
Project submission for Udacity Data Engineering Nano Degree

Author: Chris Marshall

## Project Summary

The music streaming app 'Sparkify' has grown immensely and needs to move their song and log data from their DWH (Data Warehouse) to a Data Lake.

The start up is looking to hire a data engineer to build an ETL pipeline that will pull data from their S3 (Amazon Simple Storage Service) bucket, process the data using Spark and load the data back to an S3 bucket to create a star schema. This will provide actionable imformation for Sparkify's analytical goals.


## Getting Started and Usage
* Make sure to have Python installed on your system. You can download python from the official website: [Python Downloads](https://www.python.org/downloads/)

* Run command to install necessary requirements:

    `pip install -r requirements.txt` 

* To read data from S3 bucket, process data and create a star schema and load back to S3 run the following command:

    `python etl.py`

## Directory Structure

This section provides an overview of the major files in the repository and their respective purposes and functionalities.

| File Name     | Purpose                                              |
|---------------|------------------------------------------------------|
| data   | Smaller dataset of the log and song data for testing purposes |
| dl.cfg   | Configuration file with credentials to AWS resources          |
| etl.py   |Performs ETL that pulls data from S3, processes the data with Spark and then loads them back to S3 as a star schema for Sparkify's analytical goals  |
| README.md     | Documentation for Project: Data Lake                       |

## References

[AWS Docs](https://docs.aws.amazon.com/)

[Spark Guide](https://spark.apache.org/docs/latest/sql-getting-started.html)

[PySpark Docs](https://spark.apache.org/docs/latest/api/python/index.html)