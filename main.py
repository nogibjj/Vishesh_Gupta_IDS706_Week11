from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


def main():
    # Extract
    extract()
    spark = start_spark("MatchDataAnalysis")
    # Load
    df = load_data(spark)
    describe(df)
    # Query
    query(
    spark,
    df,
    """
    SELECT Round, COUNT(*) AS match_count 
    FROM MatchData 
    GROUP BY Round 
    ORDER BY Round
    """,
    "MatchData",
    )
    # transformation
    example_transform(df)
    end_spark(spark)


if __name__ == "__main__":
    main()
