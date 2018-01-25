from python.sparkts.onsMethods.Duplicate import duplicate


def test_dm1(hive_context):

    # Input Data
    inData = "../../../src/test/resources/sml/inputs/DuplicateMarker.json"
    inDf = hive_context.read.json(inData).select("id", "num", "order")
    print("Input DataFrame")
    inDf.show()

    # Expected Data
    expData = "../../../src/test/resources/sml/outputs/DuplicateMarker.json"
    expDf = hive_context.read.json(expData).select("id", "num", "order", "marker")
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
    assert expDf.collect() == outDf.collect()
