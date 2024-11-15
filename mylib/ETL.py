import requests
from dotenv import load_dotenv
import os
import json
import base64
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, when, monotonically_increasing_id
from pyspark.sql.types import IntegerType

# Load environment variables
load_dotenv()
server_h = os.getenv("SERVER_HOSTNAME")
access_token = os.getenv("ACCESS_TOKEN")
FILESTORE_PATH = "dbfs:/FileStore/mini_project11"
headers = {'Authorization': 'Bearer %s' % access_token}
url = "https://"+server_h+"/api/2.0"

LOG_FILE = "final_pyspark_output.md"

def log_output(operation, output, query=None):
    """Logs output to a markdown file."""
    with open(LOG_FILE, "a") as file:
        file.write(f"The operation is {operation}\n\n")
        if query: 
            file.write(f"The query is {query}\n\n")
        file.write("The truncated output is: \n\n")
        file.write(output)
        file.write("\n\n")

def perform_request(path, method="POST", data=None):
    """Performs an HTTP request to the Databricks API."""
    session = requests.Session()
    response = session.request(
        method=method,
        url=f"{url}{path}",
        headers=headers,
        data=json.dumps(data) if data else None,
        verify=True
    )
    return response.json()

def upload_file_from_url(url, dbfs_path, overwrite):
    """Uploads a file from a URL to DBFS."""
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        # Create file handle
        handle = perform_request("/dbfs/create", 
                                 data={"path": dbfs_path, 
                                       "overwrite": overwrite})["handle"]
        print(f"Uploading file: {dbfs_path}")
        # Add file content in chunks
        for i in range(0, len(content), 2**20):
            perform_request(
                "/dbfs/add-block",
                data={"handle": handle, 
                      "data": base64.standard_b64encode(content[i:i+2**20]).decode()}
            )
        # Close the handle
        perform_request("/dbfs/close", data={"handle": handle})
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Failed to download")

def extract(
    source_url="https://raw.githubusercontent.com/footballcsv/england/refs/heads/master/2010s/2019-20/eng.1.csv",
    target_path=FILESTORE_PATH+"/match_data_vg157.csv",
    directory=FILESTORE_PATH,
    overwrite=True
):
    """Extracts and uploads data."""
    # Create directory on DBFS
    perform_request("/dbfs/mkdirs", data={"path": directory})
    # Upload file to DBFS
    upload_file_from_url(source_url, target_path, overwrite)
    return target_path

def transform_and_load(dataset="dbfs:/FileStore/mini_project11/match_data_vg157.csv"):
    """Transforms and loads data into Delta Lake."""
    spark = SparkSession.builder.appName("Transform and Load Match Data").getOrCreate()
    match_data_df = spark.read.csv(dataset, header=True, inferSchema=True)
    
    # Sanitize column names
    sanitized_columns = [col_name.replace(" ", "_")
                         .replace("(", "")
                         .replace(")", "")
                         .replace(".", "_") 
                         for col_name in match_data_df.columns]
    match_data_df = match_data_df.toDF(*sanitized_columns)

    # Add ID and calculate scores
    match_data_df = (match_data_df
        .withColumn("id", monotonically_increasing_id())
        .withColumn("Team1_Score", split(col("FT"), "-").getItem(0).cast(IntegerType()))
        .withColumn("Team2_Score", split(col("FT"), "-").getItem(1).cast(IntegerType()))
        .withColumn("Result", when(col("Team1_Score") > col("Team2_Score"), "Win")
                             .when(col("Team1_Score") < col("Team2_Score"), "Loss")
                             .otherwise("Draw"))
    )

    # Save as Delta table
    match_data_df.write.format("delta").mode("overwrite").saveAsTable("match_data_delta")
    num_rows = match_data_df.count()
    print(f"Number of rows in the transformed dataset: {num_rows}")
    log_output("load data", match_data_df.limit(10).toPandas().to_markdown())
    return "Transformation and loading completed successfully."

def query_transform():
    """Runs a query on the transformed data."""
    spark = SparkSession.builder.appName("Run Query").getOrCreate()
    query = """
        SELECT Round, COUNT(*) AS match_count 
        FROM match_data_delta 
        GROUP BY Round 
        ORDER BY Round
    """
    query_result = spark.sql(query)
    log_output("query data", 
               query_result.limit(10).toPandas().to_markdown(), 
               query=query)
    query_result.show()
    return query_result
