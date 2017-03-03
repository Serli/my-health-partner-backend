package model

import java.io.{FileOutputStream, ObjectOutputStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree

/**
  * Build, train and save on the disk a DecisionTree model.
  * @param trainingData the data to use to train the model
  */
class DecisionTrees(trainingData: RDD[LabeledPoint]) {

  /**
    * Create the DecisionTree model.
    * @return the DecisionTree model
    */
  def createModel() = {
    val categoricalFeaturesInfo: Map[Int, Int] = Map()
    val numClasses = 6
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 32

    val model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    val fos = new FileOutputStream("/data/model/DecisionTree.model")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close

    model
  }

}