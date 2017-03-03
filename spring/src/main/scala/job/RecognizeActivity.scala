package job

import dao.CompleteData
import org.apache.spark.mllib.tree.model.DecisionTreeModel

import scala.collection.JavaConverters._

/**
  * Recognize an activity from a java List of Complete data and a model.
  */
object RecognizeActivity {

  /**
    * Recognize an activity from a java List of CompleteData and a model.
    * @param dataJavaList the list of CompleteData to recognize
    * @param model the ML model
    * @return the List of the activity recognized
    */
  def recognizeActivity(dataJavaList: java.util.List[CompleteData], model: DecisionTreeModel): java.util.List[java.lang.Long] = {
    val features = ComputeFeature.getFeatureFromJava(dataJavaList)

    features.map(f => model.predict(f).toLong.asInstanceOf[java.lang.Long])
      .collect
      .toList.asJava
  }

}