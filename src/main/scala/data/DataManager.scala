package data

import com.datastax.spark.connector.CassandraRow
import org.apache.spark.rdd.RDD

object DataManager {

	def toDouble(data : RDD[CassandraRow]) : RDD[Array[Double]] = {
		data.map(entry => Array(entry.getDouble("acc_x"), entry.getDouble("acc_y"), entry.getDouble("acc_z")))
	}

	def withTimestamp(data : RDD[CassandraRow]) : RDD[Array[Long]] = {
		data.map(entry => Array(entry.getLong("timestamp"), entry.getLong("acc_y")))
	}

}