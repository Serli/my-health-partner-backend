package model

import com.datastax.spark.connector.toSparkContextFunctions
import job.{CreateModel, SparkContextLoader}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.scalatest._

class ModelPerformanceTest extends FunSuite with Matchers with BeforeAndAfterAll{

  var dataList : List[RDD[LabeledPoint]] = List()

  override protected def beforeAll(): Unit = {
    val sc = SparkContextLoader.sc
    val cassandraRDD = sc.cassandraTable("accelerometerdata", "feature")
    val cassandraRow = cassandraRDD.select("activity", "mean_x", "mean_y", "mean_z", "variance_x", "variance_y", "variance_z", "avg_abs_diff_x", "avg_abs_diff_y", "avg_abs_diff_z", "resultant", "avg_time_peak")

////////// All /////////////
    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

////////// - 1 //////////////
    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(
      Array(entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
        entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
        entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
        entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

////////// - 2 //////////////
    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(
      Array(entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
        entry.getDouble("resultant"),
        entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

////////// - 3 //////////////
    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("resultant"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"),
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

////////// - 4 //////////////
    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("resultant")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

    dataList = dataList :+ cassandraRow.map(entry => (entry.getLong("activity"), Vectors.dense(Array(
      entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))

  }

  test("RandomForestsTest") {
    for(data <- dataList) {
      var error: Double = 0
      var errorMax: Double = 0
      var errorMin: Double = 100
      for (_ <- 1 to 25) {
        val splits = data.randomSplit(Array(0.5, 0.5))
        val training = splits(0)
        val test = splits(1)
        val model = new RandomForests(training).createModel()
        var e = test.map(p => (model.predict(p.features), p.label))
          .filter(p => p._1 != p._2)
          .count()
          .toDouble
        e /= test.count().toDouble
        if (e > errorMax) errorMax = e
        if (e < errorMin) errorMin = e
        error += e
      }
      error /= 25
      println("Sample size : " + data.count)
      println("Feature size : " + data.first().features.size)
      println("Average error : " + error * 100 + "%")
      println("Minimal error : " + errorMin * 100 + "%")
      println("Maximal error : " + errorMax * 100 + "%")
      // assert(error < 0.1, "Average error should be < 10%")
      // assert(errorMax < 0.15, "Maximal error should be < 15%")
    }
  }

  test("DecisionTreesTest") {
    for (data <- dataList) {
      var error: Double = 0
      var errorMax: Double = 0
      var errorMin: Double = 100
      for (_ <- 1 to 25) {
        val splits = data.randomSplit(Array(0.5, 0.5))
        val training = splits(0)
        val test = splits(1)
        val model = new DecisionTrees(training).createModel()
        var e = test.map(p => (model.predict(p.features), p.label))
          .filter(p => p._1 != p._2)
          .count()
          .toDouble
        e /= test.count().toDouble
        if (e > errorMax) errorMax = e
        if (e < errorMin) errorMin = e
        error += e
      }
      error /= 25
      println("Sample size : " + data.count)
      println("Average error : " + error * 100.0 + "%")
      println("Minimal error : " + errorMin * 100 + "%")
      println("Maximal error : " + errorMax * 100 + "%")
      // assert(error < 0.1, "Average error should be < 10%")
      // assert(errorMax < 0.15, "Maximal error should be < 15%")
    }
  }

}
