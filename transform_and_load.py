from mylib.ETL import transform_and_load
import os 

if __name__ == "__main__":
    current_directory = os.getcwd()
    transform_and_load()