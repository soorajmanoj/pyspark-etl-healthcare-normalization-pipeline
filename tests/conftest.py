import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession for tests."""
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-healthcare-etl")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
