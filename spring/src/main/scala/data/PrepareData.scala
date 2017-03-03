package data

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkException

/**
  * Compute the different time windows.
  */
object PrepareData {

  def boundariesDiff(timestamps: RDD[Long], firstElement: Long, lastElement: Long): RDD[(Array[Long], Long)] = {

    val firstRDD = timestamps.filter(record => record > firstElement)
    val secondRDD = timestamps.filter(record => record < lastElement)

    try {
      val result = firstRDD.zip(secondRDD)
        .map(pair => (Array(pair._1, pair._2), pair._1 - pair._2))

      result.count

      result
    } catch {
      case _: SparkException =>
        val firstRDD_index = firstRDD.zipWithIndex().map(record => (record._2, record._1))
        val secondRDD_index = secondRDD.zipWithIndex().map(record => (record._2, record._1))

        firstRDD_index.join(secondRDD_index)
          .sortBy(pair => pair._1, true, 1)
          .map(pair => pair._2)
          .map(pair => (Array(pair._1, pair._2), pair._1 - pair._2))
    }
  }

  def defineJump(tsBoundaries: RDD[(Array[Long], Long)]): RDD[(Long, Long)] = {

    tsBoundaries.filter(pair => pair._2 > 400)
      .map(pair => (pair._1(1), pair._1(0)))
  }

  def defineInterval(tsJump: RDD[(Long, Long)], firstElement: Long, lastElement: Long, windows: Long): List[Array[Long]] = {

    val flatten = tsJump.flatMap(pair => List(pair._1, pair._2))
      .sortBy(t => t, true, 1)
      .collect

    val size = flatten.length

    var results: List[Array[Long]] = List()

    if (size > 0) {

      results :+= Array(firstElement, flatten(0), math.round((flatten(0) - firstElement) / windows.toDouble))

      for (i <- 1 until (size - 1) by 2) {
        results :+= Array(flatten(i), flatten(i + 1), math.round((flatten(i + 1) - flatten(i)) / windows.toDouble))
      }

      results :+= Array(flatten(size - 1), lastElement, math.round((lastElement - flatten(size - 1)) / windows.toDouble))

    } else {
      results :+= Array(firstElement, lastElement, (math.round(lastElement - firstElement) / windows.toDouble).toLong)
    }

    results
  }

}
