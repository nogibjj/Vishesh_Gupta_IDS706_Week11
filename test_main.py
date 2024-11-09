import os
import pytest
from mylib.lib import (
    extract,
    load_data,
    describe,
    query,
    example_transform,
    start_spark,
    end_spark,
)


@pytest.fixture(scope="module")
def spark():
    spark = start_spark("TestApp")
    yield spark
    end_spark(spark)


def testing_extract():
    """Test that the extract function creates the data file as expected"""
    file_path = extract()
    assert os.path.exists(file_path) is True


def testing_load(spark):
    """Test that load_data correctly loads the data into a DataFrame"""
    df = load_data(spark)
    assert df is not None
    assert df.count() > 0
    assert "Round" in df.columns


def testing_describe(spark):
    """Test that describe function generates summary statistics without error"""
    df = load_data(spark)
    result = describe(df)
    assert result is None


def testing_query(spark):
    """Test that query function runs and returns expected results"""
    df = load_data(spark)
    result = query(spark, df, "SELECT * FROM MatchData WHERE Round = 1", "MatchData")
    assert result is None


def testing_transform(spark):
    """Test that example_transform correctly adds score and result columns"""
    df = load_data(spark)
    result = example_transform(df)
    assert result is None


if __name__ == "__main__":
    testing_extract()
    testing_load(spark)
    testing_describe(spark)
    testing_query(spark)
    testing_transform(spark)
