package task3

import java.io.File

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, sum}

object Task3App {

  def main(args: Array[String]) {

    /**
     * Get configuration :
     */

    // Load config file :
    val config = ConfigFactory.parseFile(new File("./task3.conf")).resolve()

    val orgLogLevel   = config.getString("task3config.orgLogLevel")
    configLogger("org", orgLogLevel)

    val cleanedDFPath = config.getString("task3config.cleanedDFPath")

    val spark = getSparkSession(config)

    println("\n" + "TASK3 - Computing top (PULocationId,DOLocationId) pairs for total_amount, neighbours included :" + "\n" +
                   "-----------------------------------------------------------------------------------------------" + "\n" )

    /**
     * Compute :
     */

    // Load cleaned dataframe from disk :
    println("\nLoading cleaned dataframe from disk ...")
    val cleanedDF = spark.read.parquet(cleanedDFPath)

    println("\nAggregating dataframe ...")
    val aggregateTotalAmountsWithNeighboursDF = aggregateTotalAmountsWithNeighbours(cleanedDF, config)

    // Display top 10 ( PULocationID , DOLocationID ) pairs for total_amount, neighbours included :
    println("\nTop 10 ( PULocationID , DOLocationID ) pairs for total_amount, neighbours included :")
    aggregateTotalAmountsWithNeighboursDF.show(10)

    spark.stop()

  }


  def aggregateTotalAmountsWithNeighbours(df: DataFrame, config: Config): DataFrame = {

    val maxDebth        = config.getInt("task3config.maxDebth")
    val nbPartitions    = config.getInt("task3config.nbPartitions")

    val spark = getSparkSession(config)

    import spark.implicits._

    // Step 1 : GroupBy dataframe (as in Task2 with method "groupBy")
    println("\n--- 1 - Performing initial \"groupBy\" ...")
    val groupedDF = df
      .repartition(nbPartitions, col("PULocationID"), col("DOLocationID"))
      .groupBy(col("PULocationID"), col("DOLocationID"))
      .agg(sum(col("total_amount")).as("total_amount"))
      .orderBy(col("total_amount").desc)

    println("initial nb of partitions : " + groupedDF.rdd.getNumPartitions)

    // Step 2
    var dfDF = groupedDF

    // Initialize dataframe :
    dfDF = dfDF.withColumn("cumulated_total_amount", $"total_amount")


    println("\n--- 2 - Performing sequential joins ...")

    // Total nb of iterations, used just for display purposes :
    val nbOfIterations = (2 * maxDebth + 1)*(2 * maxDebth + 1) - 1

    var currentIteration = 1

    // this "for loop" runs through all combinations of neighbours :
    for (
      i <- -maxDebth to maxDebth;
      j <- -maxDebth to maxDebth
    )  if (!(i==0 && j==0)) // to avoid joining on the same (PULocationID , DOLocationID)
    {
      println("-------- Step " + currentIteration + "/" + nbOfIterations + " ...")
      // the dataframe is joined with the initial dataframe in order to get enriched with neighbours
      // at each join, and for each pair, the contribution of one of the neighbouring pairs (if any) is cumulated
      dfDF = joinCustom(dfDF, groupedDF, i, j, nbPartitions, config)
      // we can also use a self-join : dfDF = joinCustom(dfDF, dfDF, i, j, nbPartitions, config).cache.localCheckpoint
      // --> in the self-join case, "localCheckpoint" breaks the lineage of dfDF, and thus prevents Spark from struggling on long lineage induced by the self-join, especially if the maxDebth is high

      currentIteration = currentIteration + 1
    }

    println("\n--- 3 - Cleaning and ordering dataframe ...")
    return dfDF
      .drop("total_amount")                         // Clean
      .orderBy(col("cumulated_total_amount").desc)  // Order by

  }

  def joinCustom(df1: DataFrame, df2: DataFrame, i: Int, j: Int, nbPartitions: Int, config: Config): DataFrame = {

    val spark = getSparkSession(config)

    import spark.implicits._

    // To enhance the label of the displayed columns in DEBUG mode - see below (for example, display "PULocationID+1" instead of "PULocationID") :
    val iString = if (i > 0) "+"+i.toString else i.toString
    val jString = if (j > 0) "+"+j.toString else j.toString

    df1.as("df1")
      .join(df2.as("df2"), ($"df1.PULocationID" === $"df2.PULocationID" - i) && ($"df1.DOLocationID" === $"df2.DOLocationID" - j) , "left_outer")
      .select(
          $"df1.*",
          $"df2.total_amount".as("total_amount"+iString+jString)
        )
      .na.fill(0.0, Array("total_amount"+iString+jString))
      .withColumn("cumulated_total_amount_temp", col("cumulated_total_amount") + col("total_amount"+iString+jString) )
      .drop("cumulated_total_amount")
      .withColumnRenamed("cumulated_total_amount_temp", "cumulated_total_amount")
      .drop("total_amount"+iString+jString) // this line can be commented in DEBUG mode
      .repartition(nbPartitions, col("PULocationID"), col("DOLocationID"))

  }

  def getSparkSession(config: Config): SparkSession = {

    return SparkSession.builder
      .appName(config.getString("task3config.spark.app.name"))
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
