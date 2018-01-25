from python.sparkts.onsMethods.Melt import melt

def test_melt1(hive_context):

    # Input Data
    input_json = "../../../src/test/resources/sml/inputs/Melt.json"
    input_dataframe = hive_context.read.json(input_json)
    print("Input DataFrame")
    input_dataframe.show()

    # Expected output Data
    expected_json = "../../../src/test/resources/sml/outputs/Melt.json"
    expected_dataframe = hive_context.read.json(expected_json)
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
    assert expected_dataframe.collect() == output_dataframe.collect()
