package data

import org.scalatest._
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.rdd.RDD

class PrepareDataTest extends FunSuite with Matchers with BeforeAndAfterAll {

	var sc : SparkContext = null

	var data : RDD[Long] = null

	var firstElement : Long = 0
	
	var lastElement : Long = 0

	override protected def beforeAll(): Unit = {
		val conf = new SparkConf()
			  .setAppName("PrepareDataTest")
			  .setMaster("local[*]")

		sc = new SparkContext(conf)

		data = sc.textFile("data/test2_ts.csv", 1)
			  .map(line => line.toLong)

		firstElement = data.first
		
		lastElement  = data.sortBy(time => time, false, 1).first
	}

	override protected def afterAll(): Unit = {
    	sc.stop()
  	}

	test("boundariesDiff") {

		var result = PrepareData.boundariesDiff(data, firstElement, lastElement)

		data.count					should be (18)
		result.count				should be (17)

		result.first._1(0) 			should be (20000000)
		result.first._1(1) 			should be (10000000)
		result.first._2    			should be (10000000)

		result.take(3)(1)._1(0) 	should be (40000000)
		result.take(3)(1)._1(1) 	should be (20000000)
		result.take(3)(1)._2    	should be (20000000)


		result.take(3)(2)._1(0) 	should be (50000000)
		result.take(3)(2)._1(1) 	should be (40000000)
		result.take(3)(2)._2    	should be (10000000)
	}

	test("defineJump") {

		var boundariesDiff = PrepareData.boundariesDiff(data, firstElement, lastElement)
		var result = PrepareData.defineJump(boundariesDiff)

		result.count 				should be (4)

		result.take(4)(0)._1		should be (50000000 )
		result.take(4)(0)._2		should be (160000000)

		result.take(4)(1)._1		should be (190000000)
		result.take(4)(1)._2		should be (300000000)

		result.take(4)(2)._1		should be (360000000)
		result.take(4)(2)._2		should be (480000000)

		result.take(4)(3)._1		should be (490000000)
		result.take(4)(3)._2		should be (600000000)
	}

	test("defineInterval") {

		var boundariesDiff = PrepareData.boundariesDiff(data, firstElement, lastElement)
		var jump = PrepareData.defineJump(boundariesDiff)
		var result = PrepareData.defineInterval(jump, firstElement, lastElement, 30000000)

		result.size 				should be (5)

		result(0)(0)				should be (10000000 )
		result(0)(1)				should be (50000000 )
		result(0)(2)				should be (1)

		result(1)(0)				should be (160000000)
		result(1)(1)				should be (190000000)
		result(1)(2)				should be (1)

		result(2)(0)				should be (300000000)
		result(2)(1)				should be (360000000)
		result(2)(2)				should be (2)

		result(3)(0)				should be (480000000)
		result(3)(1)				should be (490000000)
		result(3)(2)				should be (0)

		result(4)(0)				should be (600000000)
		result(4)(1)				should be (640000000)
		result(4)(2)				should be (1)
	}
}