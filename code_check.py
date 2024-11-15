# Databricks notebook source
%pip install python-dotenv

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

from mylib.ETL import extract, transform_and_load,query_transform
from mylib.Query import spark_sql_query

# COMMAND ----------

extract()

# COMMAND ----------

transform_and_load()

# COMMAND ----------

query_transform()

# COMMAND ----------

spark_sql_query("SELECT * FROM match_data_delta")
