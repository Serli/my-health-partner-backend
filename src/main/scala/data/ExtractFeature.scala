package data

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

/**
  * Compute the features from a RDD containing the accelerometer data.
  * @param rdd the RDD containing the accelerometer data
  */
class ExtractFeature(rdd: RDD[Array[Double]]) {

  val summary: MultivariateStatisticalSummary = Statistics.colStats(rdd.map(Vectors.dense))

  /**
    * Compute the average acceleration.
    * @return the average acceleration
    */
  def computeAvgAcc(): Array[Double] = {
    summary.mean.toArray
  }

  /**
    * Compute the acceleration variance.
    * @return the variance
    */
  def computeVariance(): Array[Double] = {
    summary.variance.toArray
  }

  /**
    * Compute the average absolute difference of the accelerometer data.
    * @return the average absolute difference
    */
  def computeAvgAbsDifference() = {
    val mean = computeAvgAcc
    val abs = rdd.map(record => Array((record(0) - mean(0)).abs, (record(1) - mean(1)).abs, (record(2) - mean(2)).abs))
      .map(Vectors.dense)
    Statistics.colStats(abs).mean.toArray
  }

  /**
    * Compute the resultant acceleration from the accelerometer data.
    * @return the resultant acceleration
    */
  def computeResultantAcc() = {
    val squared = rdd.map(record => math.pow(record(0), 2) + math.pow(record(1), 2) + math.pow(record(2), 2))
      .map(math.sqrt)
      .map(sum => Vectors.dense(Array(sum)))

    Statistics.colStats(squared).mean.toArray(0)
  }

  /**
    * Compute the average time between peak from accelerometer data and the timestamp.
    * @param timestamp the timestamp
    * @return the average time between peak
    */
  def computeAvgTimeBetweenPeak(timestamp : RDD[Array[Long]]) = {
    val max = summary.max.toArray

    val filtered_y = timestamp.filter(record => record(1) > 0.9 * max(1))
      .map(record => record(0))
      .sortBy(time => time, true, 1)

    var result = 0.0

    if (filtered_y.count > 1) {
      val firstElement = filtered_y.first
      val lastElement = filtered_y.sortBy(time => time, false, 1).first

      val firstRDD = filtered_y.filter(record => record > firstElement)
      val secondRDD = filtered_y.filter(record => record < lastElement)

      try {
        val product = firstRDD.zip(secondRDD)
          .map(pair => pair._1 - pair._2)
          .filter(value => value > 0)
          .map(line => Vectors.dense(line))

        if (product.count > 0)
          result = Statistics.colStats(product).mean.toArray(0)
      } catch {
        case _: SparkException => {
          val firstRDD_index = firstRDD.zipWithIndex().map(record => (record._2, record._1))
          val secondRDD_index = secondRDD.zipWithIndex().map(record => (record._2, record._1))

          val product = firstRDD_index.join(secondRDD_index)
            .sortBy(pair => pair._1, true, 1)
            .map(pair => pair._2)
            .map(pair => pair._1 - pair._2)
            .filter(value => value > 0)
            .map(line => Vectors.dense(line))

          if (product.count > 0)
            result = Statistics.colStats(product).mean.toArray(0)
        }
      }
    }
    result
  }

}
