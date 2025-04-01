# StockETL: A PySpark-Based ETL Pipeline for Stock Data

## Overview

- StockETL is a PySpark and AWS based ETL pipeline that processes stock price and trade volume data from CSV files stored in an S3 bucket. 
- The data is transformed into wide format tables, calculates stock returns, and loads the processed data into a PostgreSQL database.
- Pipeline orchestraction occurs through AWS lambda function

## Tech Stack

- **Apache Spark**: PySpark for distributed data processing
- **AWS S3**: Storage for raw and error data
- **PostgreSQL**: AWS RDS
- **Python**: ETL Scripting
- **AWS Lambda**: Pipeline Orchestration
- **AWS Cloudwatch Event**: To watch over EMR and move files to appropriate bucket as per the status

## File Structure

.
│── lambda/
│   │── trigger_emr.py  # Lambda function to trigger EMR job on S3 file drop
│   │── monitor_emr.py   # Lambda function to monitor EMR job status
│── spark_jobs/
│   │── stock_etl.py     # PySpark ETL job script
│── stocks/
│    │── input_1.csv
│    │── input_2.csv
│    │── input_3.csv
│    │── input_4.csv
│    │── input_5.csv
│    │── input_6.csv
│    │── input_7.csv
│    │── input_8.csv
│    │── input_9.csv
│    │── input_10.csv
│── README.md
