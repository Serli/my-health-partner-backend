package data

import org.scalatest._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}


class ExtractFeatureTest extends FunSuite with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = null

  var data: RDD[Array[Double]] = null

  var data_with_ts: RDD[Array[Long]] = null

  var feature: ExtractFeature = null

  override protected def beforeAll(): Unit = {
    val conf = new SparkConf()
      .setAppName("ExtractFeatureTest")
      .setMaster("local[*]")

    sc = new SparkContext(conf)

    data = sc.textFile("data/test.csv")
      .map(_.split(","))
      .map(x => Array(x(0).toDouble, x(1).toDouble, x(2).toDouble))

    data_with_ts = sc.textFile("data/test_with_ts.csv")
      .map(_.split(","))
      .map(x => Array(x(0).toLong, x(1).toDouble.toLong))

    feature = new ExtractFeature(data)
  }

  override protected def afterAll(): Unit = {
    sc.stop()
  }


  test("computeAvgAcc") {
    var mean = feature.computeAvgAcc
      .map(x => math.round(x * 100) / 100.0)

    mean(0) should be(-5.23)
    mean(1) should be(8.11)
    mean(2) should be(1.22)
  }

  test("computeVariance") {
    var variance = feature.computeVariance
      .map(x => math.round(x * 1000) / 1000.0)

    variance(0) should be(0.022)
    variance(1) should be(0.008)
    variance(2) should be(0.013)
  }

  test("computeAvgAbsDifference") {
    var avgAbsDiff = feature.computeAvgAbsDifference()
      .map(x => math.round(x * 100) / 100.0)

    avgAbsDiff(0) should be(0.12)
    avgAbsDiff(1) should be(0.06)
    avgAbsDiff(2) should be(0.10)
  }

  test("computeResultantAcc") {
    var resultant = math.round(feature.computeResultantAcc() * 100) / 100.0
    resultant = math.round(resultant * 100) / 100.0

    resultant should be(9.73)
  }

  test("computeAvgTimeBetweenPeak") {
    var avgTimePeak = feature.computeAvgTimeBetweenPeak(data_with_ts)
    avgTimePeak = math.round(avgTimePeak * 100) / 100.0

    avgTimePeak should be(1.00)
  }

}