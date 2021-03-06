package data

import job.SparkContextLoader
import org.scalatest._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class PrepareDataTest extends FunSuite with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _

  var data: RDD[Long] = _

  var firstElement: Long = 0

  var lastElement: Long = 0

  override protected def beforeAll(): Unit = {
    sc = SparkContextLoader.sc

    data = sc.textFile("src/test/resources/test2_ts.csv", 1)
      .map(line => line.toLong)

    firstElement = data.sortBy(time => time, true, 1).first

    lastElement = data.sortBy(time => time, false, 1).first
  }

  test("boundariesDiff") {

    val result = PrepareData.boundariesDiff(data, firstElement, lastElement)

    data.count should be(18)
    result.count should be(17)

    result.first._1(0) should be(300)
    result.first._1(1) should be(100)
    result.first._2 should be(200)

    result.take(3)(1)._1(0) should be(500)
    result.take(3)(1)._1(1) should be(300)
    result.take(3)(1)._2 should be(200)


    result.take(3)(2)._1(0) should be(700)
    result.take(3)(2)._1(1) should be(500)
    result.take(3)(2)._2 should be(200)
  }

  test("defineJump") {

    val boundariesDiff = PrepareData.boundariesDiff(data, firstElement, lastElement)
    val result = PrepareData.defineJump(boundariesDiff)

    result.count should be(4)

    result.take(4)(0)._1 should be(700)
    result.take(4)(0)._2 should be(1600)

    result.take(4)(1)._1 should be(2000)
    result.take(4)(1)._2 should be(3000)

    result.take(4)(2)._1 should be(3800)
    result.take(4)(2)._2 should be(4800)

    result.take(4)(3)._1 should be(5000)
    result.take(4)(3)._2 should be(6000)
  }

  test("defineInterval") {

    val boundariesDiff = PrepareData.boundariesDiff(data, firstElement, lastElement)
    val jump = PrepareData.defineJump(boundariesDiff)
    val result = PrepareData.defineInterval(jump, firstElement, lastElement, 500)

    result.size should be(5)

    result(0)(0) should be(100)
    result(0)(1) should be(700)
    result(0)(2) should be(1)

    result(1)(0) should be(1600)
    result(1)(1) should be(2000)
    result(1)(2) should be(1)

    result(2)(0) should be(3000)
    result(2)(1) should be(3800)
    result(2)(2) should be(2)

    result(3)(0) should be(4800)
    result(3)(1) should be(5000)
    result(3)(2) should be(0)

    result(4)(0) should be(6000)
    result(4)(1) should be(6400)
    result(4)(2) should be(1)
  }
}