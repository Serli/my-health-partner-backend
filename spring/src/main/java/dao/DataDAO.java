package dao;

import java.util.List;

/**
 * Inteface with the database
 */
public interface DataDAO {
    void createKeyspace();

    void open();

    void addDataEntry(CompleteData cd);

    void addFeatureEntry(FeatureData fd);

    boolean isEmpty();

    void close();

    List<CompleteData> getData(int imei);

    List<FeatureData> getFeature(int imei);

    void deleteAllData();

    void deleteData(int imei);

    void deleteAllFeature();
}
