import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws
from pyspark.sql.types import StringType
#from data.db_connection import DBConnection

# Initialize the Spark session (optional in Databricks, as Spark is often pre-configured)
spark = SparkSession.builder.appName("DatabricksDirectQuery").getOrCreate()

def spark_sql_query(query):
    result_df = spark.sql(query)
    print("Spark SQL executed:\n", result_df.limit(10).toPandas().to_markdown())

if __name__ == "__main__":
  spark_sql_query('select * from match_data_vg157')