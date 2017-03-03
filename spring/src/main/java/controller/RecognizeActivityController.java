package controller;

import job.CreateModel;
import job.RecognizeActivity;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import dao.CompleteData;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.List;

import javax.validation.Valid;

import org.apache.spark.mllib.tree.model.DecisionTreeModel;

/**
 * Controller which receives data sent by the final application
 * Then, it return the detected activity.
 */
@RestController
@RequestMapping("/recognize")
public class RecognizeActivityController {

    private static DecisionTreeModel model = loadModel();

    /**
     * Load the stored model at start if it exits build it if none was stored.
     * @return the MLlib model
     */
    private static DecisionTreeModel loadModel() {
        DecisionTreeModel m = null;
        try {
            ObjectInputStream ois = new ObjectInputStream(new FileInputStream("/data/model/DecisionTree.model"));
            m = (DecisionTreeModel) ois.readObject();
        } catch (IOException e) {
            m = CreateModel.createModel();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return m;
    }

    /**
     * Handle a post request on /recognize.
     * The request have to contain a {@link List} of {@link CompleteData}.
     * If a model exist, it will compute and return a {@link List} of the activities detected.
     * @param completeData the data to recognize
     * @return the list of the activities recognized
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public List<Long> recognizeActivity(@RequestBody @Valid List<CompleteData> completeData) {
        if (model == null)
            model = loadModel();
        if (model != null)
            return RecognizeActivity.recognizeActivity(completeData, model);
        return null;
    }

    /**
     * Update the existing model.
     */
    public static void updateModel() {
        model = CreateModel.createModel();
    }

}
