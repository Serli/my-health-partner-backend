package dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of the interface which connects with the learning database
 */

public class DataDAOImpl implements DataDAO {
    /**
     * The cluster that we are going to use to connect to the database
     */
    Cluster cluster;

    /**
     * The session that we are going to use to connect to a keyspace
     */
    Session session;

    /**
     * Connect to the cluster and keyspace "system", creates the
     * keyspace "accelerometerdata" if it does not exist and then connect to
     * the keyspace "accelerometerdata"
     */
    @Override
    public void open() {
        try {
            cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
            session = cluster.connect("system");
        } catch (Exception e) {
            cluster = Cluster.builder().addContactPoint("172.17.0.2").build();
            session = cluster.connect("system");
        }
    }

    /**
     * Close the cluster.
     */
    @Override
    public void close() {
        cluster.close();
    }

    /**
     * Add entry in the data table.
     *
     * @param cd Entry to insert
     */
    @Override
    public void addDataEntry(CompleteData cd) {
        String cqlStatementInsert = "INSERT INTO accelerometerdata.data " +
                "(imei, height, weight, age, gender, activity, timestamp, x, y, z) " +
                " VALUES (" + cd.getImei() +
                ", " + cd.getHeight() +
                ", " + cd.getWeight() +
                ", " + cd.getAge() +
                ", " + cd.getGender() +
                ", " + cd.getActivity() +
                ", " + cd.getTimestamp() +
                ", " + cd.getX() +
                ", " + cd.getY() +
                ", " + cd.getZ() +
                ") IF NOT EXISTS;";
        session.execute(cqlStatementInsert);
    }

    /**
     * Add entry in the feature table.
     *
     * @param fd Entry ti insert
     */
    @Override
    public void addFeatureEntry(FeatureData fd) {
        String cqlStatementInsert = "INSERT INTO accelerometerdata.feature " +
                "(imei, height, weight, age, gender, activity, mean_x, mean_y, mean_z, variance_x, variance_y, variance_z, avg_abs_diff_x, avg_abs_diff_y, avg_abs_diff_z, resultant, avg_time_peak, timestamp_start, timestamp_stop)" +
                "VALUES (" + fd.getImei() +
                ", " + fd.getHeight() +
                ", " + fd.getWeight() +
                ", " + fd.getAge() +
                ", " + fd.getGender() +
                ", " + fd.getActivity() +
                ", " + fd.getMeanX() +
                ", " + fd.getMeanY() +
                ", " + fd.getMeanZ() +
                ", " + fd.getVarianceX() +
                ", " + fd.getVarianceY() +
                ", " + fd.getVarianceZ() +
                ", " + fd.getAvgAbsDiffX() +
                ", " + fd.getAvgAbsDiffY() +
                ", " + fd.getAvgAbsDiffZ() +
                ", " + fd.getResultant() +
                ", " + fd.getAvgTimePeak() +
                ", " + fd.getTimestampStart() +
                ", " + fd.getTimestampStop() +
                ") IF NOT EXISTS;";

        session.execute(cqlStatementInsert);
    }

    /**
     * Indicates if the table data is empty or not
     *
     * @return A boolean which says if the table is empty
     */
    @Override
    public boolean isEmpty() {
        String cqlStatementGet = "SELECT * FROM data;";
        ResultSet rs = session.execute(cqlStatementGet);
        return rs.all().isEmpty();
    }

    /**
     * Return an ArrayList of {@link CompleteData} object containing data for a given IMEI.
     *
     * @param imei The user's IMEI
     * @return The data for a given user IMEI
     */
    @Override
    public List<CompleteData> getData(int imei) {
        ArrayList<CompleteData> dataList = new ArrayList<CompleteData>();
        String cqlStatementGet = "SELECT * FROM data WHERE imei = " + imei + ";";
        ResultSet rs = session.execute(cqlStatementGet);
        List<Row> listRow = rs.all();

        for (int i = 0; i < listRow.size(); i++) {
            CompleteData cd = new CompleteData();
            cd.setImei(imei);
            cd.setTimestamp(listRow.get(i).getLong(1));
            cd.setHeight(listRow.get(i).getInt(2));
            cd.setWeight(listRow.get(i).getInt(3));
            cd.setAge(listRow.get(i).getInt(4));
            cd.setGender(listRow.get(i).getInt(5));
            cd.setActivity(listRow.get(i).getInt(6));
            cd.setX(listRow.get(i).getFloat(7));
            cd.setY(listRow.get(i).getFloat(8));
            cd.setZ(listRow.get(i).getFloat(9));
            dataList.add(cd);
        }

        return dataList;
    }

    /**
     * Return an List of {@link FeatureData} object containing feature for a given IMEI.
     *
     * @param imei The user's IMEI
     * @return The features for the given IMEI
     */
    @Override
    public List<FeatureData> getFeature(int imei) {
        ArrayList<FeatureData> featureList = new ArrayList<>();
        String cqlStatementGet = "SELECT * FROM feature WHERE imei = " + imei + ";";
        ResultSet rs = session.execute(cqlStatementGet);
        List<Row> listRow = rs.all();

        for (Row row : listRow) {
            FeatureData fd = new FeatureData();
            fd.setImei(imei);
            fd.setHeight(row.getInt(1));
            fd.setWeight(row.getInt(2));
            fd.setAge(row.getInt(3));
            fd.setGender(row.getInt(4));
            fd.setActivity(row.getInt(5));
            fd.setMeanX(row.getDouble(6));
            fd.setMeanY(row.getDouble(7));
            fd.setMeanZ(row.getDouble(8));
            fd.setVarianceX(row.getDouble(9));
            fd.setVarianceY(row.getDouble(10));
            fd.setVarianceZ(row.getDouble(11));
            fd.setAvgAbsDiffX(row.getDouble(12));
            fd.setAvgAbsDiffY(row.getDouble(13));
            fd.setAvgAbsDiffZ(row.getDouble(14));
            fd.setResultant(row.getDouble(15));
            fd.setAvgTimePeak(row.getDouble(16));
            fd.setTimestampStart(row.getLong(17));
            fd.setTimestampStop(row.getLong(18));
            featureList.add(fd);
        }
        return featureList;
    }

    /**
     * Delete all data in the data table.
     */
    @Override
    public void deleteAllData() {
        String cqlStatementDeleteAllData = "TRUNCATE accelerometerdata.data;";
        session.execute(cqlStatementDeleteAllData);
    }

    /**
     * Delete all data in the feature table.
     */
    @Override
    public void deleteAllFeature() {
        String cqlStatementDeleteAllFeature = "TRUNCATE accelerometerdata.feature;";
        session.execute(cqlStatementDeleteAllFeature);
    }

    /**
     * Creates the accelerometerdata keyspace.
     */
    @Override
    public void createKeyspace() {
        ResultSet results = session.execute("SELECT * FROM system_schema.keyspaces " +
                "WHERE keyspace_name = 'accelerometerdata';");

        if (results.all().isEmpty()) {
            String cqlStatementKeyspace = "CREATE KEYSPACE accelerometerdata WITH " +
                    "replication = {'class':'SimpleStrategy','replication_factor':1}";
            session.execute(cqlStatementKeyspace);

            session = cluster.connect("accelerometerdata");

            String cqlStatementTableData = "CREATE TABLE data (" +
                    " imei bigint, " +
                    " height int, " +
                    " weight int, " +
                    " age int, " +
                    " gender int, " +
                    " activity int, " +
                    " timestamp bigint, " +
                    " x float, " +
                    " y float, " +
                    " z float, " +
                    " PRIMARY KEY(imei, timestamp));";

            String cqlStatementTableFeature = "CREATE TABLE feature (" +
                    " imei bigint, " +
                    " height int, " +
                    " weight int, " +
                    " age int, " +
                    " gender int, " +
                    " activity int, " +
                    " mean_x double, " +
                    " mean_y double, " +
                    " mean_z double, " +
                    " variance_x double, " +
                    " variance_y double, " +
                    " variance_z double, " +
                    " avg_abs_diff_x double, " +
                    " avg_abs_diff_y double, " +
                    " avg_abs_diff_z double, " +
                    " resultant double, " +
                    " avg_time_peak double, " +
                    " timestamp_start bigint, " +
                    " timestamp_stop bigint, " +
                    " PRIMARY KEY(imei, timestamp_start, timestamp_stop));";

            session.execute(cqlStatementTableData);
            session.execute(cqlStatementTableFeature);
        }
    }

    public boolean keyspaceExists() {
        ResultSet results = session.execute("SELECT * FROM system_schema.keyspaces " +
                "WHERE keyspace_name = 'accelerometerdata';");
        return (!results.all().isEmpty());
    }
}
