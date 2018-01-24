import unittest
from pyspark.sql.session import SparkSession
from python.sparkts.onsMethods.Lag import lag


class TestLag(unittest.TestCase):
    """
    Class containing the test for lag function method
    """

    def test_lagFunction(self):
        """
        Method: Lag Function Test
        Version 1.0
        author: Ian Edward
        :return: Test Result
        """

        # Spark Session
        spark = SparkSession.builder\
            .master("local[*]") \
            .config("spark.driver.cores", 1)\
            .config("spark.jars", "../../../target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar")\
            .appName("Test Lag")\
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark

        # Input Data
        df_in = spark.read.json("../../../src/test/resources/sml/inputs/lag_data.json")
        print "Input DataFrame"
        df_in.show()

        # Expected Data
        df_out = spark.read.json("../../../src/test/resources/sml/outputs/expected_data.json")
        print "Expected Output DataFrame"
        exp_df_out = df_out.select("id", "t", "v", "lagged1", "lagged2")
        exp_df_out.show()

        # Input Parameters
        part_col = ["id"]
        ord_col = ["t"]
        tar_col = "v"
        lag_num = 2

        # Instantiate Class
        lag_api = lag(df_in)

        # Call API
        res_df = lag_api.lagFunc(df_in, part_col, ord_col, tar_col, lag_num)
        print "Result DataFrame"
        res_df.show()

        # Assert the results
        self.maxDiff = None
        self.assertEquals(exp_df_out.collect(), res_df.collect())
