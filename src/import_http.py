from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql import functions as F
import json

"""
This script processes the HTTP log data and campaign-reward mapping, dynamically creates a schema from an Avro schema file, 
and saves the processed data into Delta tables in the Databricks catalog.

Author: Justine Paul Padayao
"""

spark = SparkSession.builder.appName("HTTP Log Processing").getOrCreate()

schema_path = "/dbfs/FileStore/schema/http_log_schema.avsc"

with open(schema_path, 'r') as schema_file:
    avro_schema = json.load(schema_file)

def avro_type_to_spark_type(avro_type):
    """
    Map Avro types to PySpark types.

    Parameters
    ----------
    avro_type : str
        The Avro data type as a string.

    Returns
    -------
    PySparkType
        The corresponding PySpark data type (e.g., StringType, TimestampType).
    
    Raises
    ------
    ValueError
        If the Avro type is not supported.
    """
    if avro_type == "string":
        return StringType()
    elif avro_type == "timestamp":
        return TimestampType()
    else:
        raise ValueError(f"Unsupported Avro type: {avro_type}")

def avro_to_spark_schema(avro_schema):
    """
    Dynamically create a PySpark schema (StructType) based on the Avro schema.

    Parameters
    ----------
    avro_schema : dict
        The Avro schema in JSON format.

    Returns
    -------
    StructType
        A PySpark StructType schema constructed from the Avro schema.
    """
    fields = []
    for field in avro_schema['fields']:
        spark_type = avro_type_to_spark_type(field['type'])
        fields.append(StructField(field['name'], spark_type, True))
    return StructType(fields)


log_schema = avro_to_spark_schema(avro_schema)

http_log_df = spark.read.format("csv") \
    .option("delimiter", " ") \
    .schema(log_schema) \
    .load("/FileStore/http_log.txt")

http_log_df.write.format("delta").mode("overwrite").saveAsTable("test.perx_schema.http_log")

campaign_reward_mapping_df = spark.read.format("csv") \
    .option("header", "true") \
    .load("/FileStore/campaign_reward_mapping.csv")

campaign_reward_mapping_df.write.format("delta").mode("overwrite").saveAsTable("test.perx_schema.campaign_reward_mapping")
