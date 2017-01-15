package model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

class RandomForests (trainingData : RDD[LabeledPoint], testData : RDD[LabeledPoint]) {
	
	def createModel : Double = {
		val categoricalFeaturesInfo : Map[Int, Int] = Map()
		val numTrees 	= 10
		val numClasses 	= 6
		val featureSubsetStrategy = "auto"
		val impurity 	= "gini"
		val maxDepth 	= 9
		val maxBins		= 32

		val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, 12345)
	
		val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))
		
		predictionAndLabel.filter(pl => pl._1 != pl._2).count / testData.count	
	}

}