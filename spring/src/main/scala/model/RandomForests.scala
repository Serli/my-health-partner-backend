package model

import java.io.{FileOutputStream, ObjectOutputStream}
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.RandomForest

/**
  * Build, train and save on the disk a RandomForest model.
  * @param trainingData the data to use to train the model
  */
class RandomForests(trainingData: RDD[LabeledPoint]) {

  /**
    * Create the RandomForest model.
    * @return the RandomForest model
    */
  def createModel() = {
    val categoricalFeaturesInfo: Map[Int, Int] = Map()
    val numTrees = 10
    val numClasses = 3
    val featureSubsetStrategy = "auto"
    val impurity = "gini"
    val maxDepth = 9
    val maxBins = 32

    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, 12345)

    val fos = new FileOutputStream("/data/model/RandomForest.model")
    val oos = new ObjectOutputStream(fos)
    oos.writeObject(model)
    oos.close

    model
  }

}