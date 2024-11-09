import os
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, split
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

LOG_FILE = "final_pyspark_output.md"

def log_output(operation, output, query=None):
    """adds to a markdown file"""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def start_spark(appName):
    spark = SparkSession.builder.appName(appName).getOrCreate()
    return spark

def end_spark(spark):
    spark.stop()
    return "stopped spark session"

def extract(url = 'https://raw.githubusercontent.com/footballcsv/england/refs/heads/master/2010s/2019-20/eng.1.csv', file_path="data/matches.csv", directory="data"):
    if not os.path.exists(directory):
        os.makedirs(directory)
    if url:
        with requests.get(url) as r:
            with open(file_path, "wb") as f:
                f.write(r.content)
    return file_path

def load_data(spark, data="data/matches.csv", name="MatchData"):
    """load data"""
    # data preprocessing by setting schema
    schema = StructType([
        StructField("Round", IntegerType(), True),
        StructField("Date", StringType(), True),
        StructField("Team 1", StringType(), True),
        StructField("FT", StringType(), True),
        StructField("Team 2", StringType(), True)
    ])
    df = spark.read.option("header", "true").schema(schema).csv(data)
    log_output("load data", df.limit(10).toPandas().to_markdown())
    return df

def query(spark, df, query, name): 
    """queries using spark sql"""
    df = df.createOrReplaceTempView(name)
    log_output("query data", spark.sql(query).toPandas().to_markdown(), query)
    return spark.sql(query).show()

def describe(df):
    summary_stats_str = df.describe().toPandas().to_markdown()
    log_output("describe data", summary_stats_str)
    return df.describe().show()

def example_transform(df):
    """Transform to extract scores and categorize game result"""
    df = df.withColumn("Team1_Score", split(col("FT"), "-").getItem(0).cast(IntegerType()))
    df = df.withColumn("Team2_Score", split(col("FT"), "-").getItem(1).cast(IntegerType()))
    df = df.withColumn("Result", when(col("Team1_Score") > col("Team2_Score"), "Win")
                                  .when(col("Team1_Score") < col("Team2_Score"), "Loss")
                                  .otherwise("Draw"))
    log_output("transform data", df.limit(10).toPandas().to_markdown())
    return df.show()


