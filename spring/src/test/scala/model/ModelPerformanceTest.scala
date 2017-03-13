package model

import com.datastax.spark.connector.toSparkContextFunctions
import job.{CreateModel, SparkContextLoader}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.scalatest._

class ModelPerformanceTest extends FunSuite with Matchers with BeforeAndAfterAll{

  var data : RDD[LabeledPoint] = _

  override protected def beforeAll(): Unit = {
    val sc = SparkContextLoader.sc
    val cassandraRDD = sc.cassandraTable("accelerometerdata", "feature")
    data = cassandraRDD.select("activity", "mean_x", "mean_y", "mean_z", "variance_x", "variance_y", "variance_z", "avg_abs_diff_x", "avg_abs_diff_y", "avg_abs_diff_z", "resultant", "avg_time_peak")
      .map(entry => (entry.getLong("activity"), Vectors.dense(Array(entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"), entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"), entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"), entry.getDouble("resultant"), entry.getDouble("avg_time_peak")))))
      .map(entry => CreateModel.getLabeledPoint(entry._1, entry._2))
      .cache()

  }

  test("RandomForestsTest") {
    var error : Double = 0
    for(_ <- 1 to 25) {
      val splits = data.randomSplit(Array(0.5, 0.5))
      val training = splits(0)
      val test = splits(1)
      val model = new RandomForests(training).createModel()
      var e = test.map(p => (model.predict(p.features), p.label))
                   .filter(p => p._1 != p._2)
                   .count()
                   .toDouble
      e /= test.count().toDouble
      error += e
    }
    error /= 25
    println("Average error : " + error*100.0 + "%")
    assert(error < 0.1, "Average error should be < 10%")
  }

  test("DecisionTreesTest") {
    var error : Double = 0
    for(_ <- 1 to 25) {
      val splits = data.randomSplit(Array(0.5, 0.5))
      val training = splits(0)
      val test = splits(1)
      val model = new DecisionTrees(training).createModel()
      var e = test.map(p => (model.predict(p.features), p.label))
        .filter(p => p._1 != p._2)
        .count()
        .toDouble
      e /= test.count().toDouble
      error += e
    }
    error /= 25
    println("Average error : " + error*100.0 + "%")
    assert(error < 0.1, "Average error should be < 10%")
  }

}
