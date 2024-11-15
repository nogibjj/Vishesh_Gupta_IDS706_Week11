# Databricks notebook source
from mylib.ETL import ETL
from mylib.Query import spark_sql_query

# COMMAND ----------

etl = ETL()

# COMMAND ----------

etl.extract()

# COMMAND ----------

etl.transform()

# COMMAND ----------

etl.load()

# COMMAND ----------

etl.spark_sql_query("""
    SELECT Round, COUNT(*) AS match_count 
    FROM MatchData 
    GROUP BY Round 
    ORDER BY Round
    """)

# COMMAND ----------

spark_sql_query("SELECT * FROM match_data_vg157")
