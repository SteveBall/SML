from unittest import TestCase

from pyspark.sql.session import SparkSession
from python.sparkts.onsMethods.Melt import melt


class TestMelt(TestCase):

    def test_melt1(self):

        # Created Spark Session and add jar dependency
        spark = SparkSession \
            .builder \
            .master("local[*]") \
            .config("spark.driver.cores", 1) \
            .config("spark.jars", "../../../target/sparkts-0.4.0-SNAPSHOT-jar-with-dependencies.jar") \
            .appName("PythonTest") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        spark

        # Input Data
        input_json = "../../../src/test/resources/sml/inputs/Melt.json"
        input_dataframe = spark.read.json(input_json)
        print("Input DataFrame")
        input_dataframe.show()

        # Expected output Data
        expected_json = "../../../src/test/resources/sml/outputs/Melt.json"
        expected_dataframe = spark.read.json(expected_json)
        print("Expected DataFrame")
        expected_dataframe.show()

        # Create Class
        transform = melt(input_dataframe)

        # Output Data
        id_vars = ["identifier", "date"]
        value_vars = ["two", "one", "three", "four"]

        output_dataframe = transform.melt1(input_dataframe, id_vars, value_vars, "variable", "turnover") \
                                    .select("date", "identifier", "turnover", "variable") \
                                    .orderBy("date")
        print("Output DataFrame")
        output_dataframe.show()

        # Assert
        self.assertEqual(expected_dataframe.collect(), output_dataframe.collect())
