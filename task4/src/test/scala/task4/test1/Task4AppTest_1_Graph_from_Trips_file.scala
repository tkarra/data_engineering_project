package task4

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions.{max, monotonically_increasing_id}
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import task4.Task4App.{computeEdgesDF, computeEdgesRDD, computeLongestChain, computeVerticesRDD, constructGraph}

class Task4AppTest_1_Graph_from_Trips_file extends FunSuite with BeforeAndAfterEach {

  val config = ConfigFactory.parseFile(new File("./task4.conf"))
  val spark = Task4App.getSparkSession(config)

  override def beforeEach(): Unit = {
    val orgLogLevel   = config.getString("task4config.orgLogLevel")
    Task4App.configLogger("org", orgLogLevel)
  }

  override def afterEach() {
    spark.stop()
  }
  
  test("Task4AppTest_1_Graph_from_Trips_file") {

    println("\n" + "Test Task4AppTest_1_Graph_from_Trips_file :" + "\n" +
                   "-------------------------------------------" + "\n" )

    val tripsFilePath = "./src/test/scala/task4/test1/trips_full.csv"

    val tripsDF =     spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(tripsFilePath)
      .withColumn("tripId", monotonically_increasing_id())   // Add unique identifier for Trips

    // Compute Edges Dataframe :
    val edgesDF = computeEdgesDF(tripsDF, config)

    // Compute Vertices RDD :
    val verticesRDD = computeVerticesRDD(tripsDF)

    // Compute Edges RDD :
    val edgesRDD = computeEdgesRDD(edgesDF)

    // Construct the Graph :
    val graph = constructGraph(verticesRDD, edgesRDD)

    val longestDistancesDF = computeLongestChain(graph, config)

    longestDistancesDF.show(10)

    assert(longestDistancesDF.agg(max("longestDistance")).head().getDouble(0) === 20.7)

  }

}


