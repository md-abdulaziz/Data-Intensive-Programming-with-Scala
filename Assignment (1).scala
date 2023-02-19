package assignment22

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature.{MinMaxScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
import org.apache.spark.sql.functions.{desc, sum, min, max}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType, StringType}


class Assignment {
  val spark: SparkSession = SparkSession.builder()
    .appName("scala")
    .config("spark.driver.host", "localhost")
    .master("local")
    .getOrCreate()

  // suppress informational or warning log messages related to the inner working of Spark
  spark.sparkContext.setLogLevel("ERROR")

  // the data frame to be used in tasks 1 and 4
  val dataSchemaD2 = new StructType(Array(
    new StructField("a", DoubleType, true),
    new StructField("b", DoubleType, true),
    new StructField("LABEL", StringType, true)
  ))
  val dataD2Raw: DataFrame = spark.read.schema(dataSchemaD2)
    .option("header", "true").csv("data/dataD2.csv")

  // creating 'features' column for k-means model
  val vectorAssemblerD2: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b"))
    .setOutputCol("featuresUnscaled")

  val transformationPipelineD2: Pipeline = new Pipeline().setStages(Array(vectorAssemblerD2))
  val dataD2Unscaled: DataFrame = transformationPipelineD2.fit(dataD2Raw).transform(dataD2Raw)

  // scaling
  val minimumsD2: Row = dataD2Unscaled.agg(min("a"), min("b")).head
  val maximumsD2: Row = dataD2Unscaled.agg(max("a"), max("b")).head
  val scaler: MinMaxScaler = new MinMaxScaler().setInputCol("featuresUnscaled")
    .setOutputCol("features").setMax(1).setMin(0)
  val dataD2: DataFrame = scaler.fit(dataD2Unscaled).transform(dataD2Unscaled)

  // the data frame to be used in task 2

  val dataSchemaD3 = new StructType(Array(
    new StructField("a", DoubleType, true),
    new StructField("b", DoubleType, true),
    new StructField("c", DoubleType, true),
    new StructField("LABEL", StringType, true)
  ))
  val dataD3Raw: DataFrame = spark.read.schema(dataSchemaD3)
    .option("header", "true").csv("data/dataD3.csv")

  // creating 'features' column for k-means model
  val vectorAssemblerD3: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b", "c"))
    .setOutputCol("featuresUnscaled")

  val transformationPipelineD3: Pipeline = new Pipeline().setStages(Array(vectorAssemblerD3))
  val dataD3Unscaled: DataFrame = transformationPipelineD3.fit(dataD3Raw).transform(dataD3Raw)

  // scaling
  val minimumsD3: Row = dataD3Unscaled.agg(min("a"), min("b"), min("c")).head
  val maximumsD3: Row = dataD3Unscaled.agg(max("a"), max("b"), max("c")).head
  val dataD3: DataFrame = scaler.fit(dataD3Unscaled).transform(dataD3Unscaled)

  // the data frame to be used in task 3 (based on dataD2 but containing numeric labels)

  val indexer: StringIndexer = new StringIndexer().setInputCol("LABEL").setOutputCol("NUM_LABEL")
  val dataD2RawWithLabels: DataFrame = indexer.fit(dataD2Raw).transform(dataD2Raw)
    .drop("LABEL")
    .selectExpr("cast(a as Double) a", "cast(b as Double) b", "cast(NUM_LABEL as Double) label")
    .na.drop("any")

  // creating 'features' column for k-means model

  val vectorAssemblerD2Labels: VectorAssembler = new VectorAssembler()
    .setInputCols(Array("a", "b", "label"))
    .setOutputCol("featuresUnscaled")

  val transformationPipelineD2Labels: Pipeline = new Pipeline().setStages(Array(vectorAssemblerD2Labels))
  val dataD2LabelsUnscaled: DataFrame = transformationPipelineD2Labels.fit(dataD2RawWithLabels)
    .transform(dataD2RawWithLabels)

  // scaling
  val minimumsD2Labels: Row = dataD2LabelsUnscaled.agg(min("a"), min("b")).head
  val maximumsD2Labels: Row = dataD2LabelsUnscaled.agg(max("a"), max("b")).head
  val dataD2WithLabels: DataFrame = scaler.fit(dataD2LabelsUnscaled).transform(dataD2LabelsUnscaled)


  def task1(df: DataFrame, k: Int): Array[(Double, Double)] = {
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel = kmeans.fit(df)
    kmModel.clusterCenters.map(x => (
      (maximumsD2(0).asInstanceOf[Double] - minimumsD2(0).asInstanceOf[Double]) * x(0)
        + minimumsD2(0).asInstanceOf[Double],
      (maximumsD2(1).asInstanceOf[Double] - minimumsD2(1).asInstanceOf[Double]) * x(1)
        + minimumsD2(1).asInstanceOf[Double]
    ))
  }

  def task2(df: DataFrame, k: Int): Array[(Double, Double, Double)] = {
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel = kmeans.fit(df)
    kmModel.clusterCenters.map(x => (
      (maximumsD3(0).asInstanceOf[Double] - minimumsD3(0).asInstanceOf[Double]) * x(0)
        + minimumsD3(0).asInstanceOf[Double],
      (maximumsD3(1).asInstanceOf[Double] - minimumsD3(1).asInstanceOf[Double]) * x(1)
        + minimumsD3(1).asInstanceOf[Double],
      (maximumsD3(2).asInstanceOf[Double] - minimumsD3(2).asInstanceOf[Double]) * x(2)
        + minimumsD3(2).asInstanceOf[Double]
    ))
  }

  def task3(df: DataFrame, k: Int): Array[(Double, Double)] = {
    val kmeans = new KMeans().setK(k).setSeed(1L)
    val kmModel = kmeans.fit(df)
    val all_centers: Array[(Double, Double)] = kmModel.clusterCenters.map(x => (
      (maximumsD2Labels(0).asInstanceOf[Double] - minimumsD2Labels(0).asInstanceOf[Double]) * x(0)
        + minimumsD2Labels(0).asInstanceOf[Double],
      (maximumsD2Labels(1).asInstanceOf[Double] - minimumsD2Labels(1).asInstanceOf[Double]) * x(1)
        + minimumsD2Labels(1).asInstanceOf[Double]
    ))
    all_centers.foreach(println)

    val fatalGroupIndices: Array[Integer] = kmModel.transform(df)
      .select("prediction", "label").groupBy("prediction")
      .agg(sum("label").as("fatality")).orderBy(desc("fatality"))
      .select("prediction").take(2).map(x => x(0).asInstanceOf[Integer])

    val centers: Array[(Double, Double)] = Array(all_centers(fatalGroupIndices(0)),
                                                 all_centers(fatalGroupIndices(1)))

    centers
  }


    // Parameter low is the lowest k and high is the highest one.
  def task4(df: DataFrame, low: Int, high: Int): Array[(Int, Double)]  = {

    def calcSilScore(k: Int, df: DataFrame, evaluator: ClusteringEvaluator): (Int, Double) = {
      val kmeans = new KMeans().setK(k).setSeed(1L)
      val predictions = kmeans.fit(df).transform(df)
      (k, evaluator.evaluate(predictions))
    }

    val evaluator = new ClusteringEvaluator()
    val silhouetteScores = (low to high).map(x => calcSilScore(x, df, evaluator)).toArray
    silhouetteScores
  }
}