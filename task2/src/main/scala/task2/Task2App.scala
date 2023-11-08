package task2

import java.io.File

import org.apache.log4j._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import com.linkedin.relevance.isolationforest.{IsolationForest, IsolationForestModel}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.functions.{count, _}
import org.apache.spark.sql.types._

object Task2App {

  def main(args: Array[String]) {

    /**
     * Get configuration :
     */

    // Load config file :
    val config = ConfigFactory.parseFile(new File("./task2.conf")).resolve()

    val orgLogLevel   = config.getString("task2config.orgLogLevel")
    val comLogLevel   = config.getString("task2config.comLogLevel")
    configLogger("org", orgLogLevel)
    configLogger("com", comLogLevel)

    val spark = getSparkSession(config)

    val tripsFilePath   = config.getString("task2config.tripsFilePath")
    val enrichedDFPath  = config.getString("task2config.enrichedDFPath")
    val predictedDFPath = config.getString("task2config.predictedDFPath")
    val cleanedDFPath   = config.getString("task2config.cleanedDFPath")

    println("\n" + "TASK2 - Removing Outliers and computing top (PULocationId,DOLocationId) pairs for total_amount :" + "\n" +
                   "------------------------------------------------------------------------------------------------" + "\n" )

    /**
     * Prepare Dataframe and save it to disk :
     */

    if(config.getBoolean("task2config.prepareData")) {

      println("\n" + "Prepare Data :" + "\n" +
                     "--------------" )

      val enrichedDF = prepareData(config, tripsFilePath)

      // Save dataframe to disk :
      enrichedDF.write.mode(SaveMode.Overwrite).parquet(enrichedDFPath)

    }

    /**
     * Train Model and save model to disk :
     */

    if(config.getBoolean("task2config.trainModel")) {

      println("\n" + "Train Model and save model to disk :" + "\n" +
                     "------------------------------------" )

      val enrichedDF = spark.read.parquet(enrichedDFPath)

      trainAndSaveModel(enrichedDF, config)

    }

    /**
     * Predict Outliers and save predicted dataframe to disk :
     */

    if(config.getBoolean("task2config.predictOutliers")) {

      println("\n" + "Predict Outliers and save predicted dataframe to disk :" + "\n" +
                     "-------------------------------------------------------" )

      // Load enriched dataframe from disk :
      val enrichedDF = spark.read.parquet(enrichedDFPath)

      // Predict Outliers :
      val predictedDF = predict(enrichedDF, config)

      // Print number of outliers :
      val nbOfOutliers = predictedDF.filter(col("predictedLabel") === 1).count()
      println("\nNb of outliers = " + nbOfOutliers)

      // Save predicted dataframe to disk
      println("\nSaving predicted dataframe to disk ...")
      predictedDF.write.mode(SaveMode.Overwrite).parquet(predictedDFPath)

      // Show a subset of outliers
      println("\nShow a subset of outliers :")
      predictedDF
        .filter(col("predictedLabel") === 1)
        .show(25, false)

    }

    /**
     * Remove Outliers and save cleaned dataframe to disk :
     */

    if(config.getBoolean("task2config.removeOutliers")) {

      println("\n" + "Remove Outliers and save cleaned dataframe to disk :" + "\n" +
                     "----------------------------------------------------" )

      println("\nLoading predicted dataframe, removing \"trip_distance\" Outliers, and writing cleaned dataframe to disk ...")
      spark.read.parquet(predictedDFPath)
        .filter(col("predictedLabel") === 0)
        .write.mode(SaveMode.Overwrite)
        .parquet(cleanedDFPath)

    }

    /**
     * Compute top 10 ( PULocationID , DOLocationID ) pairs for total_amount :
     */

    if(config.getBoolean("task2config.computeTop10")) {

      println("\n" + "Compute top 10 ( PULocationID , DOLocationID ) pairs for total_amount :" + "\n" +
                     "-----------------------------------------------------------------------" )

      // Load cleaned dataframe from disk :
      println("\nLoading cleaned dataframe from disk ...")
      val cleanedDF = spark.read.parquet(cleanedDFPath)

      val aggregationMethod = config.getString("task2config.aggregationMethod")

      val aggregateTotalAmountsDF = aggregateTotalAmounts(cleanedDF, aggregationMethod, config)

      // Display top 10 ( PULocationID , DOLocationID ) pairs for total_amount :
      println("\nTop 10 ( PULocationID , DOLocationID ) pairs for total_amount :")
      aggregateTotalAmountsDF.show(10)

    }

    spark.stop()

  }

  def filterAndEnrichDataframe(df: DataFrame): DataFrame = {

    /**
     * In order to remove "trip_distance" outliers, the "speed" feature is used as it sounds relevant
     * In order to compute the speed, we need to compute the "trip_duration" out of the Pick-Up and Drop-Off Timestamps
     * We also filter out negative, zero and NULL values for : "trip_distance" and "trip_duration"
     * (NULL values are dropped as they are not meaningful for ML algorithm. Another option could be to replace those missing values with the "mean")
     */

    println("\nFiltering and Enriching the Trips Dataframe ...")

    return df
      .filter(col("trip_distance").isNotNull )
      .withColumn("trip_duration", col("tpep_dropoff_datetime").cast(LongType) - col("tpep_pickup_datetime").cast(LongType))   // compute "trip_duration"
      .filter(col("trip_duration").isNotNull && col("trip_duration") > 0 )                                                               // remove negative and zero durations if any
      .withColumn("speed", col("trip_distance") / col("trip_duration"))                                                        // add "speed feature"

    /**
     * Just for discussion :
     * If we needed to remove "trip_distance" AND "fare_amount" outliers, we could have
     * computed the 3 following features : "speed", "amount_per_mile", and "amount_per_second"
     * and filtered out negative and zero values for : "trip_distance", "trip_duration", and "fare_amount" :

      return df
        .filter(df.col("trip_distance").isNotNull && col("trip_distance") > 0 )
        .filter(df.col("fare_amount").isNotNull && col("fare_amount") > 0 )
        .withColumn("trip_duration", col("tpep_dropoff_datetime").cast(LongType) - col("tpep_pickup_datetime").cast(LongType))
        .filter(col("trip_duration").isNotNull && col("trip_duration") > 0 )
        .withColumn("speed", col("trip_distance") / col("trip_duration"))
        .withColumn("amount_per_mile", col("fare_amount") / col("trip_distance"))
        .withColumn("amount_per_second", col("fare_amount") / col("trip_duration"))

     */

  }

  def assembleFeaturesInDataframe(df: DataFrame, featuresColumns: Array[String]): DataFrame = {

    /**
     * Combine the features columns into one single "features" column :
     */

    println("\nAssembling features in the Trips Dataframe ...")
    if (featuresColumns.size ==1) println("(used feature =\"speed\")")
    else println("(used features =\"speed\" and \"trip_distance\")")

    val assembler = new VectorAssembler()
      .setInputCols(featuresColumns)
      .setOutputCol("features")

    return assembler
      .transform(df)

  }

  def scaleDataframe(df: DataFrame): DataFrame = {

    /**
     * Scale the features :
     */

    println("\nScaling features in the Trips Dataframe ...")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")

    val scaler_model = scaler.fit(df)

    return scaler_model
      .transform(df)
      .drop("features")
      .withColumnRenamed("scaledFeatures", "features")

  }

  def prepareData(config: Config, dataPath: String): DataFrame = {

    /**
     * Load the Trips file into a Spark Dataframe :
     */

    println("\nLoading the Trips file into a Spark Dataframe ...")
    val tripsDF = getSparkSession(config).read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", ",")
      .option("inferSchema", "true")
      .load(dataPath)

    /**
     * Filter and Enrich Dataframe :
     */

    val tripsFilteredEnrichedDF = filterAndEnrichDataframe(tripsDF)

    tripsFilteredEnrichedDF.cache()

    /**
     * Two sets of features are available :
     *    - "speed" feature alone
     *    - "speed" and "trip_distance" features
     */

    val nbFeatures      = config.getInt("task2config.nbFeatures")
    val featuresColumns = if (nbFeatures == 1) Array("speed") else Array("speed", "trip_distance")

    /**
     * Assemble Features in Dataframe :
     */

    val assembledDF = assembleFeaturesInDataframe(tripsFilteredEnrichedDF, featuresColumns )

    /**
     * Scale Dataframe :
     */

    val scaledDF = scaleDataframe(assembledDF)

    scaledDF.cache()

    println("\nScaled Dataframe schema :")
    scaledDF.printSchema()

    println("\nShow 5 rows from the scaled dataframe :")
    scaledDF.show(5, truncate = false)

    return scaledDF

  }

  def trainAndSaveModel(df: DataFrame, config: Config) {

    val modelPath = config.getString("task2config.modelPath")
    val contamination = config.getDouble("task2config.contamination")
    val maxSamples = config.getInt("task2config.maxSamples")

    /**
     * Train the model :
     */

    println("\nTraining the Isolation Forest model for Outliers Detection ...")

    val isolationForest = new IsolationForest()
      .setNumEstimators(100)
      .setBootstrap(false)
      .setMaxSamples(maxSamples)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(contamination)
      .setContaminationError(0.01 * contamination)
      .setRandomSeed(1)

    val model = isolationForest.fit(df)

    /**
     * Save the model :
     */

    model.write.overwrite().save(modelPath)

  }

  def predict(df: DataFrame, config: Config): DataFrame = {

    val modelPath = config.getString("task2config.modelPath")

    println("\nPredicting Outliers ...")

    val model = IsolationForestModel.load(modelPath)

    return model.transform(df)

  }

  def aggregateTotalAmounts(df: DataFrame, aggregationMethod: String, config: Config): DataFrame = {

    val nbPartitions = config.getInt("task2config.nbPartitions")

    val spark = getSparkSession(config)

    import spark.implicits._

    println("\nAggregating dataframe (selected aggregation method = " + aggregationMethod + ") ...")

    if (aggregationMethod == "groupBy") {

      return df
        // partitioning is very important here, in order to avoid a lot of shuffling in the groupBy :
        .repartition(nbPartitions, col("PULocationID"), col("DOLocationID"))
        .groupBy(col("PULocationID"), col("DOLocationID"))
        .agg(sum(col("total_amount")).as("total_amount"))
        .orderBy(col("total_amount").desc)

    } else if (aggregationMethod == "reduceByKey") {

      return df
        .map(line => (
          (Tuple2(line.getAs[Int]("PULocationID"), line.getAs[Int]("DOLocationID")))
          ,
          line.getAs[Double]("total_amount")
        ))
        .rdd
        .reduceByKey((x, y) => x + y)
        .toDF("[PULocationID,DOLocationID]", "total_amount")
        .orderBy(col("total_amount").desc)

    } else {
      println("Error : Please specify an aggregation method in the config file (groupBy or reduceByKey)")
      return null
    }

  }

  def getSparkSession(config: Config): SparkSession = {

    return SparkSession.builder
      .appName(config.getString("task2config.spark.app.name"))
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
