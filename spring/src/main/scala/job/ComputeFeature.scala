package job

import dao.{CompleteData, FeatureData}
import data.ExtractFeature
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
  * Compute the features for both the java and the scala code.
  */
object ComputeFeature {

  /**
    * Build a Vector containing the feature from a RDD containing the accelerometer data and a RDD containing the timestamp data.
    * @param doubles the accelerometer data
    * @param timestamp the timestamp data
    * @return a Vector containing the feature
    */
  def getFeatureVector(doubles: RDD[Array[Double]], timestamp: RDD[Array[Long]]): Vector = {
    val extractFeature: ExtractFeature = new ExtractFeature(doubles)

    val mean = extractFeature.computeAvgAcc()
    val variance = extractFeature.computeVariance()
    val avgAbsDiff = extractFeature.computeAvgAbsDifference()
    val resultant = extractFeature.computeResultantAcc()
    val avgTimePeak = extractFeature.computeAvgTimeBetweenPeak(timestamp)

    Vectors.dense(Array(mean(0), mean(1), mean(2),
      variance(0), variance(1), variance(2),
      avgAbsDiff(0), avgAbsDiff(1), avgAbsDiff(2),
      resultant,
      avgTimePeak))
  }

  /**
    * Build a RDD containing the features Vector from a java List of CompleteData.
    * @param dataJavaList the java List of CompleteData
    * @return a RDD containing the features Vector
    */
  def getFeatureFromJava(dataJavaList: java.util.List[CompleteData]): RDD[Vector] = {
    val sc = SparkContextLoader.sc

    val dataList = dataJavaList.toList

    val data = sc.parallelize(dataList)
      .map(entry => (entry.getTimestamp, entry.getX, entry.getY, entry.getZ))
      .sortBy(entry => entry._1, true, 1)
      .cache

    val times = data.map(entry => entry._1)

    val begin = times.sortBy(time => time, true, 1).first

    val end = times.sortBy(time => time, false, 1).first

    var features: List[Vector] = List()

    for (i <- begin until end-2500L by 5000L) {
      val interval = data.filter(entry => i <= entry._1 && entry._1 < (i + 5000L))
        .sortBy(entry => entry._1, true, 1)

      val doubles = interval.map(entry => Array(entry._2.toDouble, entry._3.toDouble, entry._4.toDouble))

      if(doubles.count() > 0) {
        val timestamp = interval.map(entry => Array(entry._1, entry._2.toLong))

        var feature = getFeatureVector(doubles, timestamp)

        features :+= feature
      }
    }

    sc.parallelize(features)
  }

  /**
    * Build FeatureData from a java List of CompleteData.
    * @param dataJavaList the java List of CompleteData
    * @return a List of the FeatureData builded
    */
  def getJavaFeature(dataJavaList: java.util.List[CompleteData]): java.util.List[FeatureData] = {
    val imei = dataJavaList.get(0).getImei
    val height = dataJavaList.get(0).getHeight
    val weight = dataJavaList.get(0).getWeight
    val age = dataJavaList.get(0).getAge
    val activity = dataJavaList.get(0).getActivity
    val gender = dataJavaList.get(0).getGender
    var timestamp = dataJavaList.get(0).getTimestamp

    val features = getFeatureFromJava(dataJavaList).map(vector => vector.toArray)
      .map(array => new FeatureData(imei, height, weight, age, gender, array(0), array(1), array(2), array(3), array(4), array(5), array(6), array(7), array(8), array(9), array(10), activity))
      .collect()
      .toList

    for (feature <- features) {
      feature.setTimestampStart(timestamp)
      timestamp += 5000L
      feature.setTimestampStop(timestamp)
    }

    features.asJava
  }

}
