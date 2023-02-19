package dip22.ex2

import org.apache.spark.sql.{DataFrame, SparkSession}


object Ex2Main extends App {
  // Create the Spark session
	val spark = SparkSession.builder()
                          .appName("ex2")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  // suppress informational log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "5")



  printTaskLine(1)
  // Task 1: File "data/rdu-weather-history.csv" contains weather data in csv format.
  //         Study the file and read the data into DataFrame weatherDataFrame.
  //         Let Spark infer the schema. Study the schema.
  val weatherDataFrame: DataFrame = spark
    .read
    .option("inferSchema", "true")
    .option("header", "true")
    .csv(path= "D:/TUNI/Period 2/Dip/week2/rdu-weather-history.csv")

  val weatherData = weatherDataFrame.take(3)
  weatherData.foreach(println)
  // Study the schema of the DataFrame:
  weatherDataFrame.printSchema()




  // Helper function to separate the task outputs from each other
  def printTaskLine(taskNumber: Int): Unit = {
    println(s"======\nTask $taskNumber\n======")
  }
}
