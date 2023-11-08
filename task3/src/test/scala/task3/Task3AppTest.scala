package task3

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.max
import org.scalatest.{BeforeAndAfterEach, FunSuite}

class Task3AppTest extends FunSuite with BeforeAndAfterEach {

  val config = ConfigFactory.parseFile(new File("./task3.conf"))
  val spark = Task3App.getSparkSession(config)

  override def beforeEach(): Unit = {
    val orgLogLevel   = config.getString("task3config.orgLogLevel")
    Task3App.configLogger("org", orgLogLevel)
  }

  override def afterEach() {
    spark.stop()
  }
  
  test("Task3App.aggregateTotalAmountsWithNeighbours") {

    println("\n" + "Test Task3App :" + "\n" +
                   "---------------" + "\n" )

    val testDF = spark.read.parquet("src/test/scala/task3/testDF.parquet")

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

    //val aggregatedTestDF = Task3App.aggregateTotalAmounts(testDF, "groupBy", config)
    val aggregateTotalAmountsWithNeighboursDF = Task3App.aggregateTotalAmountsWithNeighbours(testDF, config)

    println("Aggregated dataframe :" )

    aggregateTotalAmountsWithNeighboursDF.show()

    /**
    Aggregated dataframe :
    +------------+------------+----------------------+
    |PULocationID|DOLocationID|cumulated_total_amount|
    +------------+------------+----------------------+
    |         150|         251|    19.200000000000003|
    |         149|         251|    19.200000000000003|
    |         150|         250|                  16.1|
    |         150|         252|                  13.4|
    |         149|         300|                   3.0|
    +------------+------------+----------------------+
     */

    assert(aggregateTotalAmountsWithNeighboursDF.agg(max("cumulated_total_amount")).head().getDouble(0) === 19.200000000000003)

  }

}


