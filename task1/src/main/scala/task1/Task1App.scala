package task1

import java.io.File

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import com.typesafe.config._

object Task1App {

  def main(args: Array[String]) {

    // Load config file :
    val config = ConfigFactory.parseFile(new File("./task1.conf")).resolve()

    val orgLogLevel   = config.getString("task1config.orgLogLevel")
    configLogger("org", orgLogLevel)

    val spark = SparkSession.builder
      .appName(config.getString("task1config.spark.app.name"))
      .getOrCreate()


    val tripsFilePath = config.getString("task1config.tripsFilePath")

    println("\n" + "TASK1 - Discovering the NYC Yellow Taxi Trip Records Dataset - September 2019 :" + "\n" +
                   "-------------------------------------------------------------------------------" + "\n" )

    // Load the Trips file into a Spark Dataframe :
    println("\nLoading the Trips file into a Spark Dataframe ...")
    val tripsDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(tripsFilePath)
      .cache()

    println("\nDataframe Schema :")
    tripsDF.printSchema

    println("\nShow sample 10 rows :")
    tripsDF.show(10, truncate = false)

    println("\nMain Metrics (count, mean, standard deviation, min, max) :")
    tripsDF
      .describe("passenger_count", "trip_distance", "total_amount", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge")
      .show()

    println("\nShow sample 5 NULL values if any :")
    tripsDF.filter("VendorID is NULL or passenger_count is NULL or trip_distance is NULL " +
      "or total_amount is NULL or fare_amount is NULL or extra is NULL or mta_tax is NULL " +
      "or tip_amount is NULL or tolls_amount is NULL or improvement_surcharge is NULL")
      .show(5, truncate = false)

    println("\nCompute Custom Metrics :")
    tripsDF.select(
      round(sum("total_amount"),2).as("Total Amount Paid ($)"),
      round(sum("trip_distance"),2).as("Total Distance Travelled (miles)")
    ).show(truncate = false)

    println("\nEstimated Average Nb Of Passengers per trip:")
    println("(zero values for \"passenger_count\" are discarded when computing the average, as the taxi driver didn't fill this number)")
    tripsDF.filter(col("passenger_count") > 0)
      .agg(round(avg("passenger_count"),2).as("Estimated Average Nb Of Passengers"))
      .show(truncate = false)

    // "Locations" dataframe is used in order to enrich the trips dataframe with locations Zones and Boroughs :

    val locationsLookupFilePath = config.getString("task1config.locationsLookupFilePath")

    val locationsDF = spark.read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(locationsLookupFilePath)

    println("\nTop 10 Pick-Up Locations (most frequent):")
    tripsDF.groupBy(col("PULocationID"))
      .agg(count(lit(1)).as("count"))
      .orderBy(col("count").desc)
      .limit(10)
      .join(locationsDF, tripsDF("PULocationID") === locationsDF("LocationID"), "left_outer")
      .select(tripsDF("PULocationID"), col("count"), locationsDF("Zone"), locationsDF("Borough"))
      .show(truncate = false)

    println("\nTop 10 Drop-Off Locations (most frequent):")
    tripsDF.groupBy(col("DOLocationID"))
      .agg(count(lit(1)).as("count"))
      .orderBy(col("count").desc)
      .limit(10)
      .join(locationsDF, tripsDF("DOLocationID") === locationsDF("LocationID"), "left_outer")
      .select(tripsDF("DOLocationID"), col("count"), locationsDF("Zone"), locationsDF("Borough"))
      .show(truncate = false)

    // Compute Paths Dataframe :
    val pathsDF = tripsDF
      .groupBy(col("PULocationID"), col("DOLocationID"))
      .agg(count(lit(1)).as("count"))
      .cache()

    println("\nTop 10 Paths :")
    pathsDF
      .orderBy(col("count").desc)
      .limit(10)
      .join(locationsDF, tripsDF("PULocationID") === locationsDF("LocationID"), "left_outer")
      .select(tripsDF("PULocationID"), tripsDF("DOLocationID"), col("count"), locationsDF("Zone").as("PU Zone"), locationsDF("Borough").as("PU Borough"))
      .join(locationsDF, tripsDF("DOLocationID") === locationsDF("LocationID"), "left_outer")
      .select(tripsDF("PULocationID"), tripsDF("DOLocationID"), col("count"), col("PU Zone"), col("PU Borough"), locationsDF("Zone").as("DO Zone"), locationsDF("Borough").as("DO Borough"))
      .show(truncate = false)

    println("\nMetrics about Paths (count, mean, standard deviation, min, max) :")
    println("(useful for partitioning tuning)")
    pathsDF
      .describe()
      .show(truncate = false)

    spark.stop()

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
