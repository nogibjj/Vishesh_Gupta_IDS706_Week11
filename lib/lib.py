import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

LOG_FILE = "final_pyspark_output.md"

def log_output(operation, output, query=None):
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is: {operation}\n\n")
        if query:
            file.write(f"The query is: {query}\n\n")
        file.write("The truncated output is:\n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    """Initializes and returns a Spark session."""
    spark = SparkSession.builder \
        .appName(appName) \
        .config("spark.executor.userClassPathFirst", "true") \
        .getOrCreate()
    return spark

def end_spark(spark):
    """Stops the Spark session."""
    spark.stop()
    return "Spark session stopped."

def extract(url = 'https://raw.githubusercontent.com/footballcsv/england/refs/heads/master/2010s/2019-20/eng.1.csv', file_path="data/matches.csv", directory="data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    with requests.get(url) as r:
        with open(file_path, "wb") as f:
            f.write(r.content)
    return file_path

def load_data(spark, data="data/matches.csv", name="2019SeasonMatches"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("Round", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("Team1", StringType(), True),
        StructField("FT", StringType(), True),
        StructField("Team2", StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv(data)

    log_output("load data", df.limit(10).toPandas().to_markdown())

    return df

def transform_data(df):
    """Transforms the DataFrame by adding separate columns for goals scored by each team."""
    # Split the "FT" column into two separate columns: Goals_Team1 and Goals_Team2
    df = df.withColumn("Goals_Team1", split(col("FT"), "-").getItem(0).cast(IntegerType())) \
           .withColumn("Goals_Team2", split(col("FT"), "-").getItem(1).cast(IntegerType()))
    log_output("Data Transformation", df.show(truncate=False))
    return df

def run_sql_query(spark, df):
    """Executes a Spark SQL query to find the total goals scored by each team."""
    # Register the DataFrame as a temporary view
    df.createOrReplaceTempView("matches")
    query = """
    SELECT Team1 AS Team, SUM(Goals_Team1) AS TotalGoals
    FROM matches
    GROUP BY Team1
    UNION ALL
    SELECT Team2 AS Team, SUM(Goals_Team2) AS TotalGoals
    FROM matches
    GROUP BY Team2
    """
    result = spark.sql(query)
    log_output("SQL Query Execution", result.show(truncate=False), query=query)
    return result
