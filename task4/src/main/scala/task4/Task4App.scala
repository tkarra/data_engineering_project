package task4

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Edge, EdgeDirection, EdgeTriplet, Graph, VertexId}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.LongType

object Task4App {

  def main(args: Array[String]) {

    /**
     * Get configuration :
     */

    // Load config file :
    val config = ConfigFactory.parseFile(new File("./task4.conf")).resolve()

    val orgLogLevel   = config.getString("task4config.orgLogLevel")
    configLogger("org", orgLogLevel)

    val spark = getSparkSession(config)

    val tripsFilePath = config.getString("task4config.tripsFilePath")

    val computeLongestChainsInDistance = config.getBoolean("task4config.computeLongestChainsInDistance")
    val computeLongestChainsInNbOfTrips = config.getBoolean("task4config.computeLongestChainsInNbOfTrips")


    println("\n" + "TASK4 - Computing longest possible one driver's chain of trips :" + "\n" +
                   "----------------------------------------------------------------" + "\n" )

    // Load the Trips file into a Spark Dataframe :
    println("\nLoading the Trips file into a Spark Dataframe ...")
    val tripsDF =  spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(tripsFilePath)
      .withColumn("tripId", monotonically_increasing_id())   // Add unique identifier for Trips
      .cache()

    //tripsDF.printSchema()

    // Compute Edges Dataframe :
    val edgesDF = computeEdgesDF(tripsDF, config)

    // Compute Vertices RDD :
    val verticesRDD = computeVerticesRDD(tripsDF)

    // Compute Edges RDD :
    val edgesRDD = computeEdgesRDD(edgesDF)

    // Construct the Graph :
    val graph = constructGraph(verticesRDD, edgesRDD)

    if (computeLongestChainsInDistance) {

      // Compute Longest Distance :
      val longestDistancesDF = computeLongestChain(graph, config)

      println("\nLongest chain of trips in terms of \"Distance\" (in miles) : ")
      longestDistancesDF.show(10,truncate = false)

      displayLongestChain(longestDistancesDF, config)

    }

    if (computeLongestChainsInNbOfTrips) {

      // Construct graph2 from previous graph by replacing all the trips distances by 1.0 :
      val graph2 = graph.mapVertices( (vertexId, distance) => 1.0)
      val longestNbOfTripsDF = computeLongestChain(graph2, config)

      println("\nLongest chain of trips in terms of \"Number of trips done\" : ")
      longestNbOfTripsDF.show(10,truncate = false)

      displayLongestChain(longestNbOfTripsDF, config)
    }

    spark.stop()

  }

  def computeEdgesDF(tripsDF: DataFrame, config: Config): DataFrame = {

    val spark = getSparkSession(config)

    import spark.implicits._

    val maxIdleDuration = config.getDouble("task4config.maxIdleDuration")

    println("\nComputing Edges dataframe by self-joining the trips dataframe ...")

    // Compute Edges dataframe by doing a self-join on the trips dataframe
    val edgesDF = tripsDF.as("df1")
      .join(tripsDF.as("df2"),
        ($"df1.DOLocationID" === $"df2.PULocationID")
          &&
          ($"df1.tpep_dropoff_datetime" < $"df2.tpep_pickup_datetime")
          &&
          ($"df2.tpep_pickup_datetime".cast(LongType) - $"df1.tpep_dropoff_datetime".cast(LongType)).lt(maxIdleDuration)
      )
      .select(
        $"df1.tripId".as("tripIdA"),
        $"df2.tripId".as("tripIdB")
      )

    //edgesDF.show(20)

    edgesDF

  }


  def computeVerticesRDD(tripsDF: DataFrame): RDD[(VertexId, (Double))] = {

    println("\nComputing Vertices RDD ...")

    tripsDF
      .select(col("tripId"), col("trip_distance"))
      .rdd
      .map(row => (
        row(0).asInstanceOf[Number].longValue(),        // tripId: VertexId (Long)
        (
          row(1).asInstanceOf[Number].doubleValue()     // trip_distance: Double
        )
      )
      )
  }

  def computeEdgesRDD(edgesDF: DataFrame): RDD[Edge[(Long)]] = {

    println("\nComputing Edges RDD ...")

    edgesDF
      .select(
        col("tripIdA"),
        col("tripIdB")
      )
      .rdd
      .map(row => Edge(
        row(0).asInstanceOf[Number].longValue(),      // tripIdA: VertexId    (source      VertexId)
        row(1).asInstanceOf[Number].longValue(),      // tripIdB: VertexId    (destination VertexId)
        (
          1
          )
      )
      )
  }

  def computeLongestChain(graph: Graph[(Double), (Long)], config: Config): DataFrame = {

    println("\nComputing Longest Distances (Pregel-like iterative vertex-parallel execution) ...")

    val spark = getSparkSession(config)

    import spark.implicits._

    // Initialize the graph such that all vertices have a cumulated distance of 0.0, and their own id as the predecessorId :
    val initialGraph = graph.mapVertices((id, vd) => (vd, 0.0, id.toLong ))

    //initialGraph.vertices.collect().foreach(println)
    //initialGraph.edges.collect().foreach(println)

    // VD : the vertex attribute type : Here : (tripDistance: Double,   cumulatedDistance: Double, predecessorId: Long)
    // ED : the edge attribute type   : Here : (NONE)
    // A  : the Pregel message type   : Here : (tripDistance + cumulatedDistance: Double, senderId: Long)

    val initialMessage = (0.0, 0.toLong)

    // vprog: (VertexId, VD, A) => VD : the user-defined vertex program which runs on each vertex and receives the inbound message and computes a new vertex value :
    def vprog(vertexId: VertexId, vertexAttr: (Double, Double, Long), message: (Double, Long)): (Double, Double, Long) = {
      (
        vertexAttr._1,                                  // the Vertex's tripDistance always remains unchanged

        math.max(vertexAttr._2, message._1),            // the Vertex's cumulatedDistance = max ( cumulatedDistance , messages's distance )

        if (message._1 > vertexAttr._2) {               // if the messages's distance is greater that the vertex's cumulatedDistance,
          message._2                                    // then vertex's predecessorId is replaced by the sender's id
        } else vertexAttr._3                            // else, the vertex's predecessorId is kept unchanged

      )
    }

    // sendMsg: EdgeTriplet[VD, ED] => Iterator[(VertexId, A)] : a user supplied function that is applied to out edges of vertices that received messages in the current iteration :
    def sendMsg(triplet: EdgeTriplet[(Double, Double, Long), _]): Iterator[(VertexId, (Double, Long))] = {

      // we compute here if it's worth sending a message :
      if (triplet.srcAttr._1 + triplet.srcAttr._2 > triplet.dstAttr._2) {   // if the srcTrip[tripDistance + cumulatedDistance] > destTrip[cumulatedDistance]
        Iterator(                                                           // Then we send a message :
          (triplet.dstId,                                                   //    * destination vertexId
            (
              triplet.srcAttr._1 + triplet.srcAttr._2 ,                     //    * source vertex [tripDistance + cumulatedDistance]
              triplet.srcId                                                 //    * source vertex id (Long)
            )
          )
        )
      } else {
        Iterator.empty                                                      // Otherwise we do not send a message
      }
    }

    // mergeMsg: (A, A) => A : a user supplied function that takes two incoming messages of type A and merges them into a single message of type A :
    def mergeMsg(a: (Double, Long), b: (Double, Long)): (Double, Long) = {
      if (a._1 > b._1) a else b               // we use the message having the longest distance
    }

    // Compute longest-distances Graph :
    val ldGraph = initialGraph.pregel(initialMessage, activeDirection = EdgeDirection.Out)(vprog, sendMsg, mergeMsg)

    val resultDF = ldGraph.vertices
      .mapValues( value => (value._1 , value._1 + value._2 , value._3))          // add the distance of the trip itself
      .map(row => ( row._1, row._2._2, row._2._3))
      //.mapValues((a, b: (Double, Double)) => (a, b._2))
      .toDF("tripID","longestDistance","previousTripID")
      .orderBy(col("longestDistance").desc)

    return resultDF

  }

  def constructGraph(vertices: RDD[(VertexId, (Double))], edges: RDD[Edge[(Long)]]): Graph[(Double), (Long)] = {

    println("\nConstructing the Graph ...")

    val defaultVertice = (0.0)

    Graph(vertices, edges, defaultVertice)
      .cache()
  }

  def displayLongestChain(longestDistancesDF: DataFrame, config: Config): Unit = {

    val spark = getSparkSession(config)

    import spark.implicits._

    var nextTrip = longestDistancesDF.head().get(0).asInstanceOf[Long]
    var previousTrip = longestDistancesDF.head().get(2).asInstanceOf[Long]

    var longestChain = List(nextTrip)

    while ( previousTrip != nextTrip ) {

      longestChain = previousTrip :: longestChain

      val tempDF = Seq((previousTrip,nextTrip)).toDF("previous", "next")

      val newPrevious = tempDF
        .as("df1")
        .join(longestDistancesDF.as("df2"),($"df1.previous" === $"df2.tripID"),"left_outer")
        .select($"df2.previousTripID")
        .head().getLong(0)

      nextTrip = previousTrip
      previousTrip = newPrevious

    }

    println(longestChain.mkString(" > "))
  }

  def getSparkSession(config: Config): SparkSession = {

    return SparkSession.builder
      .appName(config.getString("task4config.spark.app.name"))
      .getOrCreate()
  }

  def configLogger(loggerName: String, loggerValue: String) {
    val logger = Logger.getLogger(loggerName)

    if ("debug".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.DEBUG);
    } else if ("info".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.INFO);
    } else if ("warn".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.WARN);
    } else if ("trace".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.TRACE);
    } else if ("off".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.OFF);
    } else if ("error".equalsIgnoreCase(loggerValue)) {
      logger.setLevel(Level.ERROR);
    }
  }

}
