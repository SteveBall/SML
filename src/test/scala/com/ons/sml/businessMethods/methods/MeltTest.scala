package ons.sml.businessMethods.methods
import org.apache.spark.sql.DataFrame
import uk.gov.ons.SparkTesting.TestSparkContext
import com.ons.sml.businessMethods.methods.Melt

class MeltTest extends TestSparkContext {

  test("Melt Test") {
    // Input data
    val inputJSON: String = "./src/test/resources/sml/inputs/Melt.json"
    val inputData: DataFrame = _hc.read.json(inputJSON)
    println("Input DataFrame")
    inputData.show()

    // Expected output data
    val expectedJSON: String = "./src/test/resources/sml/outputs/Melt.json"
    val expectedData: DataFrame = _hc.read.json(expectedJSON)
                                     .select("identifier", "date", "variable", "turnover")
                                     .orderBy("date")
    println("Expected output DataFrame")
    expectedData.show()

    // Create instance of Melt
    val transform = new Melt(inputData)

    // Input DataFrame going through the melt method
    val melted : DataFrame = transform.melt1(inputData,
                                             id_vars=List("identifier", "date"),
                                             value_vars=List("two","one","three","four"),
                                             var_name = "variable",
                                             value_name = "turnover")
                                      .select("identifier", "date", "variable", "turnover")
                                      .orderBy("date")

    // Checking that the output matches expected output
    assert(melted.collect() === expectedData.collect())
  }

}
