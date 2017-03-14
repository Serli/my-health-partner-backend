package data

import job.SparkContextLoader
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD


class ExtractFeatureTest extends FunSuite with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  var data: RDD[Array[Double]] = _

  var data_with_ts: RDD[Array[Long]] = _

  var feature: ExtractFeature = _

  override protected def beforeAll(): Unit = {
    sc = SparkContextLoader.sc

    data = sc.textFile("src/test/resources/test.csv")
      .map(_.split(","))
      .map(x => Array(x(0).toDouble, x(1).toDouble, x(2).toDouble))

    data_with_ts = sc.textFile("src/test/resources/test_with_ts.csv")
      .map(_.split(","))
      .map(x => Array(x(0).toLong, x(1).toDouble.toLong))

    feature = new ExtractFeature(data)
  }

  test("computeAvgAcc") {
    val mean = feature.computeAvgAcc
      .map(x => math.round(x * 100) / 100.0)

    mean(0) should be(-5.23)
    mean(1) should be(8.11)
    mean(2) should be(1.22)
  }

  test("computeVariance") {
    val variance = feature.computeVariance
      .map(x => math.round(x * 1000) / 1000.0)

    variance(0) should be(0.022)
    variance(1) should be(0.008)
    variance(2) should be(0.013)
  }

  test("computeAvgAbsDifference") {
    val avgAbsDiff = feature.computeAvgAbsDifference()
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