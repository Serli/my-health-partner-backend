package job

import data.{DataManager, ExtractFeature, PrepareData}
import model.{DecisionTrees, RandomForests}
import com.datastax.spark.connector.CassandraRow
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import com.datastax.spark.connector.SparkContextFunctions

object RecognizeActivity {

	val activities : List[String] = List("Standing", "Jogging", "Walking", "Sitting", "Upstairs", "Downstairs")

	def main(args: Array[String]): Unit = {

		// Define spark configuration
		val sparkConf = new SparkConf()
						.setAppName("User's physical activity recognition")
						.setMaster("local[*]")

		// Initiate spark context
		val sc = new SparkContext(sparkConf)

        val rdd = sc.textFile("data/data.csv")
                        .mapPartitionsWithIndex((idx, x) => if (idx == 0) x.drop(1) else x)
                        .map(_.split(","))
                        .filter(record => record.length == 6)
                        .map(record => (record(0).toInt, record(1), record(2).toLong, record(3).toDouble, record(4).toDouble, record(5).toDouble))

		var labeledPoints : List[LabeledPoint] = List()

        val userIds = rdd.map(record => record._1)
                         .collect

		for( i <- userIds) {
			for( activity <- activities) {
			  	val times : RDD[Long] = rdd.map(record => record._3)
			  
                if(times.count > 100) {

                    var intervals = defineWindows(times)
                    
                    for(interval <- intervals){
                        for( j <- 0L until interval(2) ) {
                    
                       	    var data = rdd.map(record => (record._3, record._4, record._5, record._6))

                            if (data.count > 0 ) {
                                // Transform into double array
                                var doubles   = data.map(record => Array(record._2, record._3, record._4))
                                // Transform into vector without timestamp
                                var vectors   = doubles.map(Vectors.dense)
                                // Data with only timestamp and acc
                                var timestamp = data.map(record => Array(record._1, record._3.toLong))

                                // Extract features from this windows
                                var extractFeature : ExtractFeature = new ExtractFeature(vectors)
                      	
                                // The average acceleration
                                var mean = extractFeature.computeAvgAcc()
                                // The variance
                                var variance = extractFeature.computeVariance()
                                // The average absolute diffrence
                                var avgAbsDiff = extractFeature.computeAvgAbsDifference(doubles)
                                // The average resultant acceleration
                                var resultant = extractFeature.computeResultantAcc(doubles)
                                // The average time between peaks
                                var avgTimePeak = extractFeature.computeAvgTimeBetweenPeak(timestamp)
                                // Let's build LabeledPoint, the structure used in MLlib to create and a predictive model
                                var labeledPoint = getLabeledPoint(activity, mean, variance, avgAbsDiff, resultant, avgTimePeak)
                                
                                labeledPoints :+= labeledPoint
                       	    }
                        }
                    }
    			}
    		}
    	}

        // ML part with the models: create model prediction and train data on it //
        if (labeledPoints.size > 0) {
       
            var data = sc.parallelize(labeledPoints)
            // Split data into 2 sets : training (60%) and test (40%)
            var splits = data.randomSplit(Array (0.6, 0.4)) 
            var trainingData = splits(0).cache
            var testData = splits(1)
     
            // With DecisionTree
            var errDT : Double = new DecisionTrees(trainingData, testData).createModel(sc)
            // With Random Forest
            var errRF : Double = new RandomForests(trainingData, testData).createModel;

            println("sample size " + data.count);
            println("Test Error Decision Tree: " + errDT);
            println("Test Error Random Forest: " + errRF)
        }
    }

    def defineWindows(times : RDD[Long]) : List[Array[Long]] = {
        // first find jumps to define the continuous periods of data
        var firstElement : Long  = times.sortBy(time => time, true, 1).first;
        var lastElement  : Long = times.sortBy(time => time, false, 1).first;

        // compute the difference between each timestamp
        var tsBoundariesDiff = PrepareData.boundariesDiff(times, firstElement, lastElement);

        // define periods of recording
        // if the difference is greater than 100 000 000, it must be different periods of recording
        // ({min_boundary, max_boundary}, max_boundary - min_boundary > 100 000 000)
        var jumps = PrepareData.defineJump(tsBoundariesDiff);
        
        // Now define the intervals
        PrepareData.defineInterval(jumps, firstElement, lastElement, 5000000000L)
    }

    /**
    * build the data set with label & features (11)
    * activity, mean_x, mean_y, mean_z, var_x, var_y, var_z, avg_abs_diff_x, avg_abs_diff_y, avg_abs_diff_z, res, peak_y
    */
    def getLabeledPoint ( activity : String, mean : Array[Double], variance : Array[Double], avgAbsDiff : Array[Double], resultant : Double, avgTimePeak:Double) : LabeledPoint = {
       var features = Array(mean(0),
                            mean(1),
                            mean(2),
                            variance(0),
                            variance(1),
                            variance(2),
                            avgAbsDiff(0),
                            avgAbsDiff(1),
                            avgAbsDiff(2),
                            resultant,
                            avgTimePeak  ) 

        //Now the label: by default 0 for Walking
        var label : Double = 0

        if (  "Jogging"  == activity) 
            label = 1
            
        if (  "Standing" == activity) 
            label = 2
             
        if (  "Sitting"  == activity) 
            label = 3
            
        if (  "Upstairs" == activity) 
            label = 4

        if ("Downstairs" == activity) 
            label = 5

        LabeledPoint(label, Vectors.dense(features))
    }

}