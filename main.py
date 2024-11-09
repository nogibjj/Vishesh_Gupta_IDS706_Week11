from lib.lib import start_spark, end_spark, extract, load_data, transform_data, run_sql_query

def main():
    spark = start_spark("2019SeasonMatches")

    # Extract 
    file_path = extract()
    # Load 
    df = load_data(spark, data=file_path)
    # Transform 
    transformed_df = transform_data(df)

    # Execute SQL 
    result_df = run_sql_query(spark, transformed_df)
    end_spark(spark)

if __name__ == "__main__":
    main()
