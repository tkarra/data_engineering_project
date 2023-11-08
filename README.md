# General Presentation 

Analysis of taxi trips data : data loading, outliers detection, graph parallel computation (Spark GraphX)

Publicly available dataset containing NYC taxi trips : https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page

We are going to work on Yellow taxi trip records for September of 2019.

For every task prepare a separate Apache Spark application written in Scala with clear instructions how to use it. Results can be printed just to the stdout.

* Task #1 : Calculate metrics/dimensions which are the most suitable in order to understand and get familiar with the dataset.

* Task #2 : Remove trip_distance outliers (using a relevant algorithm) from provided dataset and calculate top 10 (PULocationId, DOLocationId) pairs for total_amount. 

* Task #3 : Calculate the same as in previous step, only lets assume the pair includes values from neighboured numbers, i.e. pair (5,5) includes (4,4), (4,5), (5,4),
(5,5), (4,6), (6,4), (5,6), (6,5), (6,6)

* Task #4 : Let's assume that the same driver starts every next trip in the same locationId less than 5 minutes later after drop-off, if these conditions are not met it means it is a different driver. Given that, calculate the longest possible one driver's chain of trips from provided dataset.

Each Task is addressed in a dedicated Spark application (Scala). Detailed instructions on how to run these applications are presented below.


# Presentation of the Spark Projets 

There are 4 Scala projects (sbt), each one dealing with its corresponding task, and having its own config file.

Here is a brief description of the main functionality in each project :

* task1 : displays (stdout) dimensions / metrics to get familiar with the dataset

* task2 : it is a pipeline composed of the following steps :
   - prepareData : loads trips csv data, filters, enriches (to add model's feature) and scales data, then produces a corresponding parquet file ("enrichedDFPath")
   - trainModel : loads prepared data from disk and trains the outliers detection model (Isolation Forest) then saves model to disk 
   - predictOutliers : loads prepared data from disk, predicts outliers, then produces a corresponding parquet file ("predictedDFPath")
   - removeOutliers : loads predicted dataframe from disk, filters out outliers, then produces a "cleaned" parquet file ("cleanedDFPath")
   - computeTop10 : loads cleaned data from disk, computes (PULocationId, DOLocationId) pairs for "total_amount", then displays the top 10 
   ==> Intermediate files allows to re-run only a part of this pipeline. The parts that are run can be configured in the config file

* task3 : 
   - loads the cleaned data (produced by task2) from disk, 
   - aggregates (PULocationId, DOLocationId) pairs for "total_amount" as in task2 (using groupBy)
   - adds the neighbours contributions to each pair (using joins)
   - displays the top 10

* task4 :
   - loads a trips csv file (we do not use the "cleaned" data in order to make the testing of this task easier )
   - builds a graph (Spark GraphX) where each vertex is a trip 
   - computes longest possible one driver's chains of trips (using Pregel iterative graph-parallel computation)
   - displays top 10 chains

# Pre-requisites

* Spark version 2.3.0

# Build

Each project can be built into a fat jar with :

```sh
sbt assembly
```

From the IDE, in order to run the main class or run unit tests, please specify the spark.master in the VM Options (IntelliJ) of the Configuration ("Edit Configurations"): 
```sh
VM options : -Dspark.master=local[*]
```

# Usage

You can test the tasks with the following :
(as explained above there is a dependency between task2 and task3, so please run the tasks in the following order)

```sh
export SPARK_HOME=<SPARK_HOME>

# Use spark-submit to run the applications :

# Task1 :
$SPARK_HOME/bin/spark-submit \
  --class "task1.Task1App" \
  --master local[4] \
  task1-assembly-1.1.jar

# Task2 :
$SPARK_HOME/bin/spark-submit \
  --class "task2.Task2App" \
  --master local[4] \
  task2-assembly-1.1.jar

# Task3 :
$SPARK_HOME/bin/spark-submit \
  --class "task3.Task3App" \
  --master local[4] \
  task3-assembly-1.1.jar
   
# Task4 :
$SPARK_HOME/bin/spark-submit \
  --class "task4.Task4App" \
  --master local[4] \
  task4-assembly-1.1.jar

```

** resource files (dataset file & config file) are supposed to be in the same folder as the jar file **

# Specific configurations

## Task2 :

__Outliers detection :__

For the Outliers detection, I used the Isolation Forest model.
This model has the following characteristics :
* linear-time complexity 
* low memory requirements
* fast (does not use any distance computation)
* scales up well, even with high dimensional problems

In this task we suggest to use this model with only one or two features, relevant to "trip_distance" outliers detection (the choice can be made in the config file):
* 1 feature : speed (trip_distance normalized with the trip_duration)
* 2 features : speed and trip_distance


__Aggregation :__

Two aggregation methods are implemented : groupBy and reduceByKey (the choice can be made in the config file)


## Task4 :

I have used a graph representation to compute the "longest possible one driver's chain of trips" :

A "son" trip is linked to its "father" if those two conditions are met :
* sonTrip.pickup_location = fatherTrip.dropoff_location
AND
* 0 < sonTrip.pickup_time - fatherTrip.dropoff_time < 5 minutes (300 s)

Thus, data is represented as a "Directed Acyclic, vertex-labelled (with trip distance) graph"

I used the pregel-like iterative vertex-parallel execution implemented in Spark GraphX

The Longest Chain of trips can be considered :
* either in terms of "maximum distance travelled"
* or in terms of "maximum number of trips done"

Settings are provided in the config file to select one of them or both of them



