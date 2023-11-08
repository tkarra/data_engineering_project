package task2

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.functions.{max, min}
import org.scalatest.{BeforeAndAfterEach, FunSuite}


class Task2AppTest extends FunSuite with BeforeAndAfterEach {

  //val config = ConfigFactory.load()
  val config = ConfigFactory.parseFile(new File("./task2.conf"))
  val spark = Task2App.getSparkSession(config)

  override def beforeEach(): Unit = {
    val orgLogLevel   = config.getString("task2config.orgLogLevel")
    Task2App.configLogger("org", orgLogLevel)
  }

  override def afterEach() {
    spark.stop()
  }
  
  test("Task2App.aggregateTotalAmounts") {

    println("\n" + "Test Task2App :" + "\n" +
                   "---------------" + "\n" )

    val testDF = spark.read.parquet("src/test/scala/task2/testDF.parquet")

    println("Initial test dataframe :" )

    testDF.show()

    /**

    Initial test dataframe :
    +------------+------------+------------+
    |PULocationID|DOLocationID|total_amount|
    +------------+------------+------------+
    |         150|         250|         3.3|
    |         150|         251|         3.1|
    |         149|         300|         3.0|
    |         149|         251|         7.2|
    |         150|         250|         2.5|
    |         150|         252|         3.1|
    +------------+------------+------------+
     */

    val aggregatedTestDF = Task2App.aggregateTotalAmounts(testDF, "groupBy", config)

    println("Aggregated dataframe :" )

    aggregatedTestDF.show()

    /**
    Aggregated dataframe :
    +------------+------------+------------+
    |PULocationID|DOLocationID|total_amount|
    +------------+------------+------------+
    |         149|         251|         7.2|
    |         150|         250|         5.8|
    |         150|         251|         3.1|
    |         150|         252|         3.1|
    |         149|         300|         3.0|
    +------------+------------+------------+
     */

    assert(aggregatedTestDF.agg(max("total_amount")).head().getDouble(0) === 7.2)

  }

}


