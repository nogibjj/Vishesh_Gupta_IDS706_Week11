import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, concat_ws
from pyspark.sql.types import StringType
import pandas as pd

class ETL:

    def __init__(self):
        # Initialize Spark session
        self.spark = SparkSession.builder.appName("ETL University Data").getOrCreate()
        self.df_spark = None

    def log_output(self, operation, output, query=None):
        LOG_FILE = "final_output.md"
        """Adds log information to a markdown file."""
        with open(LOG_FILE, "a") as file:
            file.write(f"## {operation}\n\n")
            if query: 
                file.write(f"### Query:\n```\n{query}\n```\n\n")
            file.write("### Output:\n\n")
            file.write(f"```\n{output}\n```\n\n")

    def extract(self, url="https://raw.githubusercontent.com/footballcsv/england/refs/heads/master/2010s/2019-20/eng.1.csv"):
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            # Load data into a Spark DataFrame
            self.df_spark = self.spark.createDataFrame(data)
            output = self.df_spark.limit(10).toPandas().to_markdown() 
            self.log_output("Data Extracted", output)
            print("Data extracted successfully.", output)
        else:
            error_message = f"Failed to fetch data. Status code: {response.status_code}"
            self.log_output("Extract Failed", error_message)
            print(error_message)

    def transform(self):
        df = df.withColumn("Team1_Score", split(col("FT"), "-")
                       .getItem(0).cast(IntegerType()))
        df = df.withColumn("Team2_Score", split(col("FT"), "-")
                          .getItem(1).cast(IntegerType()))
        df = df.withColumn("Result", when(col("Team1_Score") > col("Team2_Score"), "Win")
                                      .when(col("Team1_Score") < col("Team2_Score"), "Loss")
                                      .otherwise("Draw"))
        self.log_output("transform data done successfully", df.limit(10).toPandas().to_markdown())
        print("Data transformed successfully.")
        
    def load(self, table_name = "MatchData"):
        """Loads the transformed data into a Databricks SQL table."""
        try:
            # Write the DataFrame to Databricks as a managed table
            self.df_spark.write.format("delta").mode("overwrite").saveAsTable(table_name)
            print(f"Data loaded into {table_name} successfully.")
            self.log_output("Data Load", f"Data loaded into {table_name} successfully.")
        except Exception as e:
            error_message = f"Error loading data: {e}"
            self.log_output("Load Failed", error_message)
            print(error_message)

    def spark_sql_query(self, query):
        self.df_spark.createOrReplaceTempView("match_data_vg157")
        result_df = self.spark.sql(query)
        self.log_output("query data", result_df.limit(10).toPandas().to_markdown(), query=query)
        print("Spark SQL query executed successfully.")
    