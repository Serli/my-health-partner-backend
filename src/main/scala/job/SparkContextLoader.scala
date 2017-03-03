package job

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Define the spark context for the application.
  */
object SparkContextLoader {

  val sparkConf = new SparkConf()
    .setAppName("My Health Partner")
    .set("spark.cassandra.connection.host", "172.17.0.2")
    .setMaster("local[*]")

  val sc = new SparkContext(sparkConf)

}
