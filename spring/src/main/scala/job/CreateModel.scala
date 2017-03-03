package job

import data.PrepareData
import model.{DecisionTrees, RandomForests}
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel

/**
  * Create a MLlib model from the database data.
  */
object CreateModel {

  /**
    * Create a MLlib model from the feature data.
    * If no feature data was found, try to create it from raw accelerometer data instead.
    *
    * @return The model created
    */
  def createModel() = {
    val sc = SparkContextLoader.sc

    val cassandraRDD = sc.cassandraTable("accelerometerdata", "feature")

    val labeledPoints = cassandraRDD.select("activity", "mean_x", "mean_y", "mean_z", "variance_x", "variance_y", "variance_z", "avg_abs_diff_x", "avg_abs_diff_y", "avg_abs_diff_z", "resultant", "avg_time_peak")
      .map(entry => (entry.getLong("activity"), Vectors.dense(Array(entry.getDouble("mean_x"), entry.getDouble("mean_y"), entry.getDouble("mean_z"), entry.getDouble("variance_x"), entry.getDouble("variance_y"), entry.getDouble("variance_z"), entry.getDouble("avg_abs_diff_x"), entry.getDouble("avg_abs_diff_y"), entry.getDouble("avg_abs_diff_z"), entry.getDouble("resultant"), entry.getDouble("avg_time_peak")))))
      .map(entry => getLabeledPoint(entry._1, entry._2))

    var result: DecisionTreeModel = null

    if (labeledPoints.count() > 0) {
      result = new DecisionTrees(labeledPoints).createModel()
      //result = new RandomForests(labeledPoints).createModel()
    }
    else {
      result = createModelFromRaw()
    }

    result
  }

  /**
    * Create a MLlib model from the raw accelerometer data.
    *
    * @return The model created
    */
  def createModelFromRaw() = {
    val sc = SparkContextLoader.sc

    val cassandraRowsRDD = sc.cassandraTable("accelerometerdata", "data")

    var labeledPoints: List[LabeledPoint] = List()

    val userIds = cassandraRowsRDD.select("imei")
      .map(entry => entry.getLong("imei"))
      .distinct
      .collect

    val activities = cassandraRowsRDD.select("activity")
      .map(entry => entry.getLong("activity"))
      .distinct
      .collect

    for (i <- userIds) {
      System.err.println("Traitement de l'user " + i)
      for (activity <- activities) {

        val times: RDD[Long] = cassandraRowsRDD.select("timestamp")
          .where("imei=? AND activity=?", i, activity)
          .map(entry => entry.getLong("timestamp"))
          .sortBy(time => time, true, 1)
          .cache

        if (times.count > 100) {

          val intervals = defineWindows(times)

          for (interval <- intervals) {
            for (j <- 0L until interval(2)) {

              val data = cassandraRowsRDD.select("timestamp", "x", "y", "z")
                .where("imei = ? AND timestamp < ? AND timestamp >= ?", i, interval(0) + (j + 1) * 5000L, interval(0) + j * 5000L)
                .withAscOrder
                .cache

              if (data.count > 0) {
                // Transform into double array
                val doubles = data.map(entry => Array(entry.getDouble("x"), entry.getDouble("y"), entry.getDouble("z")))
                // Data with only timestamp and acc
                val timestamp = data.map(entry => Array(entry.getLong("timestamp"), entry.getLong("x")))

                val features = ComputeFeature.getFeatureVector(doubles, timestamp)

                var labeledPoint = getLabeledPoint(activity, features)

                labeledPoints :+= labeledPoint
              }
            }
          }
        }
      }
    }

    var decisionTrees: DecisionTreeModel = null
    //        var randomForests : RandomForests

    if (labeledPoints.nonEmpty) {

      val trainingData = sc.parallelize(labeledPoints)

      System.err.println("Training data size = " + trainingData.count())

      decisionTrees = new DecisionTrees(trainingData).createModel()
      // randomForests = new RandomForests(trainingData).createModel()

    }

    decisionTrees
  }

  /**
    * Define the time windows in a timestamp RDD.
    * A time window is defined as a start timestamp, an end timestamp and the number of sequence it's containing.
    *
    * @param times the timestamp RDD
    * @return a List of times windows
    */
  def defineWindows(times: RDD[Long]): List[Array[Long]] = {
    val firstElement: Long = times.first
    val lastElement: Long = times.sortBy(time => time, false, 1).first

    val tsBoundariesDiff = PrepareData.boundariesDiff(times, firstElement, lastElement)

    val jumps = PrepareData.defineJump(tsBoundariesDiff)

    PrepareData.defineInterval(jumps, firstElement, lastElement, 5000L)
  }

  /**
    * Build a LabeledPoint from an activity label and a features vector.
    *
    * @param activity the label of the point
    * @param features the vector of the point
    * @return a LabeledPoint
    */
  def getLabeledPoint(activity: Long, features: Vector): LabeledPoint = {
    LabeledPoint(activity.toDouble, features)
  }

}