from mylib.ETL import extract
#, transform_and_load,query_transform
#from mylib.Query import spark_sql_query
import os 

if __name__ == "__main__":
    current_directory = os.getcwd()
    extract()
    #transform_and_load()
    #query_transform()
    #spark_sql_query("SELECT * FROM match_data_delta")
