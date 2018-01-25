import pytest
import logging

from pyspark.sql import HiveContext
from pyspark import SparkConf, SparkContext

def quiet_py4j():
    logger = logging.getLogger('py4j')
    logger.setLevel(logging.WARN)

@pytest.fixture(scope="session")
def spark_context(request):
    conf = SparkConf().setMaster("local[*]").setAppName("Pytesting") \
        .set("spark.jars", "../../../target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar")
    sc = SparkContext(conf=conf)
    request.addfinalizer(lambda: sc.stop())
    quiet_py4j()
    return sc

@pytest.fixture(scope="session")
def hive_context(spark_context):
    return HiveContext(spark_context)

