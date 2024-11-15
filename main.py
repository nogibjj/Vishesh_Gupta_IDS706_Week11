from mylib.ETL import ETL
from mylib.Query import spark_sql_query

def main():
    etl = ETL()
    etl.extract()
    etl.transform()
    etl.load()
    etl.spark_sql_query("""
    SELECT Round, COUNT(*) AS match_count 
    FROM MatchData 
    GROUP BY Round 
    ORDER BY Round
    """)


    #db_conn.close()
    spark_sql_query("SELECT * FROM match_data_vg157") #WHERE state_province IS NOT NULL

if __name__ == "__main__":
    main()
