from unittest import TestCase

from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf

from python.sparkts.onsMethods.StandardError import standardError


class TestStandardError(TestCase):
    """
    This class holds the tests for the pyspark API for the standard error function. So far the test just checks
    whether the class and objects work correctly together.
    """
    def test_stdErr1(self):
        """
        This checks whether calling the class object with the correct inputs returns the right data back, this is
        checked through an assert with the expected data.

        It does this by first creating the SQLContext, then in reads in the input and the expected data into 2
        dataframes, the expected also has a select operation on it to get the columns in the right order.

        After this the class object is created with the input data, then the method is called with the right parameters
        and the input data.
        The expected data and the output data are then compared to see if they should match.
        Each dataframe is printed out in order to check manually as well as programmatically.
        :return: N/A
        """

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
