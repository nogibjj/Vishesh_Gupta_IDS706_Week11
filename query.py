from mylib.ETL import query_transform
from mylib.Query import spark_sql_query
import os 

if __name__ == "__main__":
    current_directory = os.getcwd()
    query_transform()
    spark_sql_query("SELECT * FROM match_data_delta")