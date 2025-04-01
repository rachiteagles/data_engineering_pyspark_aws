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

```
│── lambda/
│   │── trigger_emr.py   # Lambda function to trigger EMR job on S3 file drop
│   │── monitor_emr.py   # Lambda function to monitor EMR job status
│── spark_jobs/
│   │── stock_etl.py     # PySpark ETL job script
│── stocks/              # Directory containing input files
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
```

## Data Flow

1. Data Ingestion (S3)
- Stock data are uploaded as CSV files to an AWS S3 bucket.
- AWS Lambda (trigger_emr.py) detects the new file drop event and triggers the AWS EMR cluster to process the data.

2. Data Processing (AWS EMR + PySpark)
- EMR runs the PySpark job (stock_etl.py) to: Read raw CSV files from input bucket
- Perform data cleaning (handle missing values, remove duplicates)
- Transform data into wide format (e.g., pivot tables with stock returns)
- Compute stock returns based on price changes
- Store cleaned and transformed data in rds database

3. Job Monitoring (Lambda + CloudWatch)
- AWS Lambda (monitor_emr.py) listens for CloudWatch Events related to EMR job status.
- If EMR job fails, files are moved to failed s3 bucket for debugging.
- If EMR job succeeds, files are moved to success s3 bucket.
- In either case, file is delete from the input bucket

4. Data Loading (PostgreSQL)
- PySpark writes transformed stock data to an AWS RDS PostgreSQL database:
- Stock Prices Table (prices)
- Stock Returns Table (returns)
- Stock Volume Table (volume)

5. Logging and Monitoring
- AWS CloudWatch logs EMR job execution details.
- Pyspark logs are stored in logs bucket through EMR
