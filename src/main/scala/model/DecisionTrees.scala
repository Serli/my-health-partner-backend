package model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.model.DecisionTreeModel

class DecisionTrees (trainingData : RDD[LabeledPoint], testData : RDD[LabeledPoint]) {

	def createModel(sc : SparkContext) : Double = {
		val categoricalFeaturesInfo : Map[Int, Int] = Map()
		val numClasses 	= 6
		val impurity 	= "gini"
		val maxDepth 	= 9
		val maxBins		= 32

		val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

		model.save(sc, "actitracker")

		val predictionAndLabel = testData.map(p => (model.predict(p.features), p.label))

		predictionAndLabel.filter(pl => pl._1 != pl._2).count / testData.count
	}

}