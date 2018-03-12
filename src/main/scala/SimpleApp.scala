/* SimpleApp.scala */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD._
import org.apache.spark.SparkContext._

object SimpleApp {

  //This is just a test to see if comment is picked up
  def main(args: Array[String]) {

    val logFile = "README.md" // Should be some file on your system

    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("SparkExamples")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    //val spark = SparkSession.builder.appName("Simple Application").master("local").getOrCreate()
    //val spark = SparkSession.builder.appName("Sp_LogistcRegression").master("local").getOrCreate

    //Create a dataset. Make it available as a cluster-wide in-memory cache
    //val dataSet = spark.read.textFile(logFile).cache()
    val dataSet = sc.textFile(logFile).cache()

    println("Number of items in DataSet: " +dataSet.count())
    println("First item (line) in DataSet: " +dataSet.first)

    //------------------------------------------------------------------------------
    //FILTER - Transform the data - return a DataSet that contains only lines with the word "Spark"
    val filterDataSet = dataSet.filter( line => line.contains("Spark"))

    //Now, print that data set by passing the in-line function to the 'foreach' method.
    println ("All lines that contain the word - Spark")
    filterDataSet.foreach(line => println(line))

    //-------------------------------------------------------------------
    //MAP - Transform the data - Run a function against each element, in this case the number of words in each line
    //NOTE: reduceByKey does not seem to work if we are using a SparkSession ...ugh!!

    //val counts = filterDataSet.flatMap(line => line.split(" "))
    //  .map(word => (word, 1))
    //  .reduceByKey(_ + _)

    //flatMap - pass it a function that returns a sequence, for flatMap, each item can be mapped to 0 or more output items
    val flatMapDataSet = filterDataSet.flatMap(line => line.split(" "))
    println("This is the flatmap data set")
    flatMapDataSet.foreach(println(_))

    //Next step...Map function applies a mapping to each element...and word in this case. Then returns a tuple
    val mapDataSet = flatMapDataSet.map(word => (word,1))
    println("This is the mapped data set")
    mapDataSet.foreach(println(_))

    //Finally, we reduceByKey. Note that 'reduceByKey (_ + _) is same as 'reduceByKey (val1, val2) (val1 + val2)'
    val reducedDataSet = mapDataSet.reduceByKey(_ + _)
    println("This is the reduced data set")
    reducedDataSet.foreach(println(_))

    //Now, print that data set by passing the in-line function to the 'foreach' method.
    println ("The sizes of all the lines")
    //flatMapDataSet.foreach(line => println(line))

    // Run an action - run count() action on that dataset
    //println("The number of lines that contain the word - Spark" +flatMapDataSet.count())

    val numAs = dataSet.filter(line => line.contains("a")).count()
    val numBs = dataSet.filter(line => line.contains("b")).count()

    println(s"Lines with a: $numAs, Lines with b: $numBs")

    sc.stop()
  }
}