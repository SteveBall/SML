from unittest import TestCase

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

from python.sparkts.onsMethods.StandardError import standardError

class TestStandardError(TestCase):
    def test_stdErr1(self):

        # Create an SQLContext
        conf = SparkConf().setMaster("local[*]").setAppName("StandardErrorTest")\
            .set("spark.jars", "../../../target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar")
        sc = SparkContext(conf=conf)
        sqlContext = SQLContext(sc)

        # Input Data
        inData = "../../../src/test/resources/sml/inputs/StandardErrorDataIn.json"
        inDf = sqlContext.read.json(inData)
        print("Input Data")
        inDf.show()

        # Expected data
        expectedData = "../../../src/test/resources/sml/outputs/StandardErrorExpected.json"
        expectedDf = sqlContext.read.json(expectedData).select("ref", "xColumn", "yColumn", "zColumn", "stdError")
        print("Expected Data")
        expectedDf.show()

        # Create the class object
        stdErrorObj = standardError(inDf)

        # Calling the method to get the resulted dataframe
        resultDf = stdErrorObj.stdErr1(df=inDf, xCol="xColumn", yCol="yColumn", zCol="zColumn", outputCol="stdError")
        print("Result Data")
        resultDf.show()

        # Assert they are equal
        self.assertEqual(expectedDf.collect(), resultDf.collect())
