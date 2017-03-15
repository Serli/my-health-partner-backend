package controller;

import dao.DataDAO;
import dao.DataDAOImpl;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import javax.validation.Valid;

import dao.CompleteData;
import dao.FeatureData;
import job.ComputeFeature;
import org.springframework.context.annotation.Scope;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller which receives data sent by the developer application
 * Then, it stores it in the learning database
 */
@Scope("request")
@RestController
@RequestMapping("/data")
public class DataController {

    private static AtomicInteger cpt = new AtomicInteger(0);

    private final DataDAO dao = new DataDAOImpl();

    /**
     * Handle a post request on /data.
     * The request have to contain a {@link List} of {@link CompleteData}.
     * The data send will be stored in the database, the feature of these data will be computed and also stored in the database.
     * @param completeData the {@link List} of {@link CompleteData} to store.
     */
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public void insertData(@RequestBody @Valid List<CompleteData> completeData) {
        dao.open();

        completeData.forEach(dao::addDataEntry);
        List<FeatureData> features = ComputeFeature.getJavaFeature(completeData);
        features.forEach(dao::addFeatureEntry);
        if (cpt.updateAndGet(n -> n + features.size()) > 100) {
            RecognizeActivityController.updateModel();
            cpt.set(0);
        }

        dao.close();
    }
}
