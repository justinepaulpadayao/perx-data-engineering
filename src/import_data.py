"""
Author: Justine Paul Padayao
Description: This script imports CSV data into Databricks and stores it in Delta tables under the specified catalog and schema 
for efficient querying. Initially, AWS Redshift was considered for data storage, but due to AWS credit card issues, 
Databricks Community Edition was used instead. The data is saved to a `test` catalog with the provided schema and table names.

Files:
    - campaign.csv
    - reward_campaigns.csv
    - reward_transactions.csv

Data is read from Databricks File System (DBFS) and written as Delta tables under the `test_catalog`.
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Data Engineering").getOrCreate()

campaign_df = spark.read.csv('dbfs:/FileStore/campaign.csv', header=True, inferSchema=True)
reward_campaign_df = spark.read.csv('dbfs:/FileStore/reward_campaign.csv', header=True, inferSchema=True)
reward_transaction_df = spark.read.csv('dbfs:/FileStore/reward_transaction.csv', header=True, inferSchema=True)

campaign_df.write.format("delta").mode("overwrite").saveAsTable("test.perx_schema.campaign")
reward_campaign_df.write.format("delta").mode("overwrite").saveAsTable("test.perx_schema.reward_campaign")
reward_transaction_df.write.format("delta").mode("overwrite").saveAsTable("test.perx_schema.reward_transaction")