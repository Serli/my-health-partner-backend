package data

import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

class ExtractFeature(rdd : RDD[Vector]) {

	val summary : MultivariateStatisticalSummary = Statistics.colStats(rdd)

	def computeAvgAcc() : Array[Double] = {
		summary.mean.toArray
	} 

	def computeVariance() : Array[Double] = {
		summary.variance.toArray
	}

	def computeAvgAbsDifference(data : RDD[Array[Double]]) = {
		val mean = computeAvgAcc
		var abs = data.map(record => Array((record(0) - mean(0)).abs, (record(1) - mean(1)).abs, (record(2) - mean(2)).abs) )
					.map(Vectors.dense)
		Statistics.colStats(abs).mean.toArray
	}

	def computeResultantAcc(data : RDD[Array[Double]]) = {
		var squared = data.map(record => math.pow(record(0), 2)
										+math.pow(record(1), 2)
										+math.pow(record(2), 2) )
						.map(math.sqrt)
						.map(sum => Vectors.dense(Array(sum)))

		Statistics.colStats(squared).mean.toArray(0)
	}

	def computeAvgTimeBetweenPeak(data : RDD[Array[Long]]) = {
		var max = summary.max.toArray

		var filtered_y = data.filter(record => record(1) > 0.9 * max(1))
						 .map(record => record(0))
						 .sortBy(time => time, true, 1)

		if(filtered_y.count > 1) {
			var firstElement = filtered_y.first
			var lastElement = filtered_y.sortBy(time => time, false, 1).first

			var firstRDD  = filtered_y.filter(record => record > firstElement)
			var secondRDD = filtered_y.filter(record => record < lastElement)

			var product = firstRDD.zip(secondRDD)
								  .map(pair => pair._1 - pair._2)
								  .filter(value => value > 0)
								  .map(line => Vectors.dense(line))

			Statistics.colStats(product).mean.toArray(0)
		} else {
			0.0
		}
	}

}