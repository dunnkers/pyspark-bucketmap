from typing import Dict, List
import pytest
from pyspark.sql import DataFrame
from pyspark.sql.column import Column
from pyspark_bucketmap import BucketMap
from pyspark.sql.functions import lit
from pyspark import Row, SparkConf
from pyspark.sql import SparkSession


@pytest.fixture
def spark() -> SparkSession:
    conf: SparkConf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("testing")
        .set("spark.sql.shuffle.partitions", "1")
    )
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


def test_pyspark_bucketmap(spark: SparkSession):
    # Define a dataset
    people: DataFrame = spark.createDataFrame(
        [
            Row(name="Damian", age=12),
            Row(name="Jake", age=15),
            Row(name="Dominic", age=18),
            Row(name="John", age=20),
            Row(name="Jerry", age=27),
            Row(name="Jerry's Grandpa", age=101),
        ]
    )

    # Configure BucketMap
    splits: List[float] = [-float("inf"), 0, 12, 18, 25, 70, float("inf")]
    mapping: Dict[int, Column] = {
        0: lit("Not yet born"),
        1: lit("Child"),
        2: lit("Teenager"),
        3: lit("Young adulthood"),
        4: lit("Adult"),
        5: lit("Elderly"),
    }
    bucket_mapper = BucketMap(
        splits=splits, mapping=mapping, inputCol="age", outputCol="phase"
    )

    # Transform
    phases_actual: DataFrame = bucket_mapper.transform(people).select("name", "phase")
    phases_expected: DataFrame = spark.createDataFrame(
        [
            Row(name="Damian", phase="Teenager"),
            Row(name="Jake", phase="Teenager"),
            Row(name="Dominic", phase="Young adulthood"),
            Row(name="John", phase="Young adulthood"),
            Row(name="Jerry", phase="Adult"),
            Row(name="Jerry's Grandpa", phase="Elderly"),
        ]
    )

    # Assert
    assert sorted(phases_actual.collect()) == sorted(phases_expected.collect())
