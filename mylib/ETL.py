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


def perform_query(path, headers, data={}):
    session = requests.Session()
    resp = session.request('POST', url + path, 
                           data=json.dumps(data), 
                           verify=True, 
                           headers=headers)
    return resp.json()


def mkdirs(path, headers):
    _data = {}
    _data['path'] = path
    return perform_query('/dbfs/mkdirs', headers=headers, data=_data)
  

def create(path, overwrite, headers):
    _data = {}
    _data['path'] = path
    _data['overwrite'] = overwrite
    return perform_query('/dbfs/create', headers=headers, data=_data)


def add_block(handle, data, headers):
    _data = {}
    _data['handle'] = handle
    _data['data'] = data
    return perform_query('/dbfs/add-block', headers=headers, data=_data)


def close(handle, headers):
    _data = {}
    _data['handle'] = handle
    return perform_query('/dbfs/close', headers=headers, data=_data)


def put_file_from_url(url, dbfs_path, overwrite, headers):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content
        handle = create(dbfs_path, overwrite, headers=headers)['handle']
        print("Putting file: " + dbfs_path)
        for i in range(0, len(content), 2**20):
            add_block(handle, 
                      base64.standard_b64encode(content[i:i+2**20]).decode(), 
                      headers=headers)
        close(handle, headers=headers)
        print(f"File {dbfs_path} uploaded successfully.")
    else:
        print(f"Error downloading file from {url}. Status code: {response.status_code}")


def extract(
    url="https://raw.githubusercontent.com/footballcsv/england/refs/heads/master/2010s/2019-20/eng.1.csv",
    file_path=FILESTORE_PATH+"/match_data_vg157.csv",
    directory=FILESTORE_PATH,
    overwrite=True
):
    """Extract a url to a file path"""
    # Make the directory, no need to check if it exists or not
    mkdirs(path=directory, headers=headers)
    # Add the csv files, no need to check if it exists or not
    put_file_from_url(url, file_path, overwrite, headers=headers)

    return file_path

def transform_and_load(dataset="dbfs:/FileStore/mini_project11/match_data_vg157.csv"):
    # Initialize Spark session
    spark = SparkSession.builder.appName("Transform and Load Match Data").getOrCreate()

    # Load dataset
    match_data_df = spark.read.csv(dataset, header=True, inferSchema=True)

    # Sanitize column names (replace spaces and invalid characters with underscores)
    sanitized_columns = [col_name.replace(" ", "_").replace("(", "").replace(")", "").replace(".", "_") for col_name in match_data_df.columns]
    match_data_df = match_data_df.toDF(*sanitized_columns)

    # Add a unique ID column
    match_data_df = match_data_df.withColumn("id", monotonically_increasing_id())

    # Transform the data to extract scores and categorize game results
    match_data_df = match_data_df.withColumn(
        "Team1_Score", split(col("FT"), "-").getItem(0).cast(IntegerType())
    )
    match_data_df = match_data_df.withColumn(
        "Team2_Score", split(col("FT"), "-").getItem(1).cast(IntegerType())
    )
    match_data_df = match_data_df.withColumn(
        "Result",
        when(col("Team1_Score") > col("Team2_Score"), "Win")
        .when(col("Team1_Score") < col("Team2_Score"), "Loss")
        .otherwise("Draw")
    )

    # Save the transformed data as a Delta table
    match_data_df.write.format("delta").mode("overwrite").saveAsTable("match_data_delta")

    # Print the number of rows
    num_rows = match_data_df.count()
    print(f"Number of rows in the transformed dataset: {num_rows}")

    return "Transformation and loading completed successfully."

def query_transform():
    spark = SparkSession.builder.appName(" Run Query").getOrCreate()
    query = (
        """
    SELECT Round, COUNT(*) AS match_count 
    FROM MatchData 
    GROUP BY Round 
    ORDER BY Round
    """
    )
    query_result = spark.sql(query)
    print(query_result.show())
    return query_result

    