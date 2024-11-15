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


    spark_sql_query("SELECT * FROM match_data_vg157")

if __name__ == "__main__":
    main()
