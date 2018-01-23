from unittest import TestCase

from pyspark.sql.session import SparkSession
from python.sparkts.onsMethods.Duplicate import duplicate


class TestDuplicate(TestCase):

    def test_dm1(self):

        # Created Spark Session
        spark = SparkSession\
            .builder\
            .master("local[*]")\
            .config("spark.driver.cores", 1) \
            .config("spark.jars", "../../../target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar") \
            .appName("JavaTest")\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark

        # Input Data
        inData = "../../../src/test/resources/sml/inputs/DuplicateMarker.json"
        inDf = spark.read.json(inData).select("id", "num", "order")
        print("Input DataFrame")
        inDf.show()

        # Expected Data
        expData = "../../../src/test/resources/sml/outputs/DuplicateMarker.json"
        expDf = spark.read.json(expData).select("id", "num", "order", "marker")
        print("Expected DataFrame")
        expDf.show()

        # Create Class
        dup = duplicate(inDf)

        # Output Data

        partCol = ["id", "num"]
        ordCol = ["order"]

        outDf = dup.dm1(inDf, partCol, ordCol, "marker")
        print("Output DataFrame")
        outDf.show()

        # Assert
        self.assertEqual(expDf.collect(), outDf.collect())
