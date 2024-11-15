from databricks import sql
import os
from dotenv import load_dotenv

class DBConnection:
    def __init__(self):
        load_dotenv()  # Load environment variables from .env file
        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = sql.connect(
                server_h = os.getenv("SERVER_HOSTNAME")
                access_token = os.getenv("ACCESS_TOKEN")
                http_path = os.getenv("HTTP_PATH")
            )
            self.cursor = self.connection.cursor()
            print("Connected to Databricks SQL database.")
        except Exception as e:
            print(f"Error connecting to Databricks SQL database: {e}")
            raise

    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        print("Connection closed.")