package task4

import java.io.File

import com.typesafe.config.ConfigFactory
import org.apache.spark.graphx.{Edge, VertexId}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import task4.Task4App.{computeLongestChain, constructGraph, displayLongestChain}
import org.apache.spark.sql.functions.{max}


class Task4AppTest_2_Graph_from_Vertices_and_Edges_file extends FunSuite with BeforeAndAfterEach {

  val config = ConfigFactory.parseFile(new File("./task4.conf"))
  val spark = Task4App.getSparkSession(config)

  val computeLongestChainsInDistance = config.getBoolean("task4config.computeLongestChainsInDistance")
  val computeLongestChainsInNbOfTrips = config.getBoolean("task4config.computeLongestChainsInNbOfTrips")

  override def beforeEach(): Unit = {
    val orgLogLevel   = config.getString("task4config.orgLogLevel")
    Task4App.configLogger("org", orgLogLevel)
  }

  override def afterEach() {
    spark.stop()
  }
  
  test("Task4AppTest_2_Graph_from_Vertices_and_Edges_file") {

    println("\n" + "Test Task4AppTest_2_Graph_from_Vertices_and_Edges_file :" + "\n" +
                   "--------------------------------------------------------" + "\n" )

    val verticesFilePath = "./src/test/scala/task4/test2/vertices.csv"

    val verticesRDD: RDD[(VertexId, (Double))] = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(verticesFilePath)
      .rdd
      .map(row => (
        row(0).asInstanceOf[Number].longValue(),
        row(1).asInstanceOf[Number].doubleValue()
      ))

    /*
    // In case we need to save Vertices csv file from DF :
    vertices.toDF("tripId", "trip_distance")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("./src/test/scala/task4/test2/vertices.csv")
    */

    val edgesFilePath = "./src/test/scala/task4/test2/edges.csv"

    val edgesRDD: RDD[Edge[(Long)]] = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(edgesFilePath)
      .rdd
      .map(row => (
        Edge(
          row(0).asInstanceOf[Number].longValue(),
          row(1).asInstanceOf[Number].longValue(),
          row(2).asInstanceOf[Number].longValue()
        )
        ))

    /*
    // In case we need to save Edges csv file from DF :
    edges.toDF("tripA", "tripB", "nb")
      .coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .save("./src/test/scala/task4/test2/edges.csv")
    */

    // Construct the Graph :
    val graph = constructGraph(verticesRDD, edgesRDD)

    /**
     * Compute Longest chain of trips in terms of "Distance" :
     */

    val longestDistancesDF = computeLongestChain(graph, config)

    println("\nLongest chain of trips in terms of \"Distance\" (in miles) : ")
    longestDistancesDF.show(10,truncate = false)

    displayLongestChain(longestDistancesDF, config)


    assert(longestDistancesDF.agg(max("longestDistance")).head().getDouble(0) === 89.0)


    /**
     * Compute Longest chain of trips in terms of "Nb of Trips" :
     */

    // Construct graph2 from previous graph by replacing all the trips distances by 1.0 :
    val graph2 = graph.mapVertices( (vertexId, distance) => 1.0)
    val longestNbOfTripsDF = computeLongestChain(graph2, config)

    println("\nLongest chain of trips in terms of \"Number of trips done\" : ")
    longestNbOfTripsDF.show(10,truncate = false)

    assert(longestNbOfTripsDF.agg(max("longestDistance")).head().getDouble(0) === 6.0)

  }



}


