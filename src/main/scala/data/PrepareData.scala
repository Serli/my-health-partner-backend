package data

import org.apache.spark.rdd.RDD

object PrepareData {

	def boundariesDiff(timestamps : RDD[Long], firstElement : Long, lastElement : Long) : RDD[(Array[Long],Long)] = {

		var firstRDD  = timestamps.filter(record => record > firstElement)
		var secondRDD = timestamps.filter(record => record < lastElement )

		var firstRDD_index  = firstRDD.zipWithIndex().map(record => (record._2,record._1))
		var secondRDD_index = secondRDD.zipWithIndex().map(record => (record._2,record._1))

		firstRDD_index.join(secondRDD_index)
					  .map(pair => pair._2)
					  .map(pair => (Array(pair._1, pair._2), pair._1 - pair._2))

		// firstRDD.zip(secondRDD)
		// 		.map(pair => (Array(pair._1, pair._2), pair._1 - pair._2))
	}

	def defineJump(tsBoundaries : RDD[(Array[Long], Long)]) : RDD[(Long, Long)] = {

		var result = tsBoundaries.filter(pair => pair._2 > 100000000)
					.map(pair => (pair._1(1), pair._1(0)))

		result
	}

	def defineInterval(tsJump : RDD[(Long, Long)], firstElement : Long, lastElement : Long, windows : Long) : List[Array[Long]] = {

		var flatten = tsJump.flatMap(pair => List(pair._1, pair._2))
							.sortBy(t => t, true, 1)
							.collect

		var size = flatten.size

		var results : List[Array[Long]] = List()

		if(size > 0) {

			results :+= Array(firstElement, flatten(0), (math.round((flatten(0) - firstElement)/windows.toDouble)).toLong)

			for ( i <- 1 until (size - 1) by 2 ) {
				results :+= Array(flatten(i), flatten(i + 1), (math.round((flatten(i + 1) - flatten(i))/windows.toDouble)).toLong)
			}
			
			results :+= Array(flatten(size - 1), lastElement, (math.round((lastElement - flatten(size - 1))/windows.toDouble)).toLong)

		} else {
			results :+= Array(firstElement, lastElement, (math.round(lastElement - firstElement)/windows.toDouble).toLong)
		}

		results
	}

}