package ex3

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.language.postfixOps
import scala.sys.process._


object Ex3Main extends App {
	val spark = SparkSession.builder()
                          .appName("ex3")
                          .config("spark.driver.host", "localhost")
                          .master("local")
                          .getOrCreate()

  // suppress log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  spark.conf.set("spark.sql.shuffle.partitions", "5")



  printTaskLine(1)
  // Task 1: File "data/sales_data_sample.csv" contains sales data of a retailer.
  //         Study the file and read the data into DataFrame retailerDataFrame.
  //         NOTE: the resulting DataFrame should have 25 columns
  val  retailerDataFrame: DataFrame = spark.read
    .option("inferSchema", true)
    .option("delimiter",";")
    .option("header", true)
    .csv("data/sales_data_sample.csv")
  retailerDataFrame.printSchema()


  printTaskLine(2)
  // Task 2: Find the best 10 selling days. That is the days for which QUANTITYORDERED * PRICEEACH
  //         gets the highest values.
  retailerDataFrame.createOrReplaceTempView("retailerDataFrame")
  println("Showing the best 10 selling days")
  val best10DaysDF: DataFrame = spark.sql(
    """
                       SELECT ORDERDATE, SUM(QUANTITYORDERED * PRICEEACH) AS TOTALSALESPERDAY
                       FROM retailerDataFrame
                       GROUP BY ORDERDATE
                       ORDER BY TOTALSALESPERDAY DESC
                       limit 10
                       """)
  best10DaysDF.show()



  printTaskLine(3)
  // Task 3: The classes that takes a type just like a parameter are known to be Generic
  //         Classes in Scala. Dataset is an example of a generic class. Actually, DataFrame is
  //         a type alias for Dataset[Row], where Row is given as a type parameter. Declare your
  //         own case class Sales with two members: year and euros of type integer. The
  //         class must be declared before this object (Ex3Main).

  //         Then instantiate a Dataset[Sales] and query for the sales on 2019 and
  //         the year with the highest amount of sales.

  import spark.implicits._
  case class Sales(year : Int, euros: Int)
  val salesList = List(Sales(2015, 325), Sales(2016, 100), Sales(2017, 15), Sales(2018, 1000),
                     Sales(2019, 50), Sales(2020, 750), Sales(2021, 950), Sales(2022, 400))
  val salesDS: Dataset[Sales] = spark.createDataset(salesList).as[Sales]

  val sales2019: Sales = salesDS.filter("year == 2019").take(1)(0)
  println(f"Sales for 2019 is ${sales2019.euros}")

  val maximumSales: Sales = salesDS.orderBy(desc("euros")).take(1)(0)
  println(f"Maximum sales: year = ${maximumSales.year}, euros = ${maximumSales.euros}")



  printTaskLine(4)
  // Task 4: Continuation from task 3.
  //         The new sales list "multiSalesList" contains sales information from multiple sources
  //         and thus can contain multiple values for each year. The total sales in euros for a year
  //         is the sum of all the individual values for that year.
  //         Query for the sales on 2019 and the year with the highest amount of sales in this case.
  val multiSalesList = salesList ++ List(Sales(2016, 250), Sales(2017, 600), Sales(2019, 75),
                                         Sales(2020, 225), Sales(2016, 350), Sales(2017, 400))
  val multiSalesDS: Dataset[Sales] = spark.createDataset(multiSalesList).as[Sales]

  val multiSales2019: Sales = multiSalesDS.filter("year == 2019").take(1)(0)
  println(f"Total sales for 2019 is ${multiSales2019.euros}")

  val maximumMultiSales: Sales = multiSalesDS.orderBy(desc("euros")).take(1)(0)
  println(f"Maximum total sales: year = ${maximumMultiSales.year}, euros = ${maximumMultiSales.euros}")



  printTaskLine(5)
  // Task 5: In the streaming version of the analysis, the streaming data will be added
  //         into the directory streamingData. The streaming data is similar to the one
  //         in the directory "data". It is just divided into multiple files.
  //
  //         Create a DataFrame that will work with streaming data
  //         that is given in the same format as for the static retailerDataFrame.
  //         Hint: Spark cannot infer the schema of streaming data, so you have to give it explicitly.
  //
  //         Note: you cannot really test this task before you have also done the tasks 6 and 7.
 val retailerStreamingDF: DataFrame =spark.readStream
  .option("sep", ",")
  .schema(repoFiles)
    .csv("cmd /C copy streamingDataRepo\\${filename} streamingData\\${filename}.csv")




  printTaskLine(6)
  // Task 6: Find the best selling days in the streaming data
  val bestDaysDFStreaming = spark.sql(
    """
                         SELECT ORDERDATE, SUM(QUANTITYORDERED * PRICEEACH) AS TOTALSALESPERDAY
                         FROM retailerStreamingDF
                         GROUP BY ORDERDATE
                         ORDER BY TOTALSALESPERDAY DESC
                         limit 1
                         """)

  bestDaysDFStreaming.show();

  printTaskLine(7)
  // Task 7: Test your solution by writing the 10 best selling days to stdout
  //         whenever the DataFrame changes

  ???

  // You can test your solution by uncommenting the following code snippet.
  // The loop adds a new CSV file to the directory "streamingData" every 5th second.
  // If you rerun the test, remove all the CSV files first from the directory "streamingData".
  // You may need to wait for a while to see the stream processing results while running the program.

  //  val repoFiles = "ls streamingDataRepo" !!
  //
  //  for(filename <- repoFiles.split("\n")){
  //	  val copyCommand = f"cp streamingDataRepo/${filename} streamingData/${filename}.csv"
  //    val _ = copyCommand !!
  //
  //    Thread.sleep(5000)
  //  }

  // NOTE: In Windows environment, use the following modified code snippet:

  val repoFiles = "cmd /C dir /b streamingDataRepo" !!

   for(filename <- repoFiles.split("\r\n"))
     {
       val copyCommand = f"cmd /C copy streamingDataRepo\\${filename} streamingData\\${filename}.csv"
      val _ = copyCommand !!

      Thread.sleep(5000)
    }



  // Stop the Spark session
  spark.stop()

  def printTaskLine(taskNumber: Int): Unit = {
    println(s"======\nTask $taskNumber\n======")
  }
}
