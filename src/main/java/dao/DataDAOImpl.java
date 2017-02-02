/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import java.util.ArrayList;
import java.util.List;
import service.CompleteData;

/**
 * Implementation of the interface which connects with the learning database
 * @author nathan
 */

public class DataDAOImpl implements DataDAO{
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
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();   
        session = cluster.connect("accelerometerdata");
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
     * @param cp Entry to insert
     */
    @Override
    public void addEntry(CompleteData cp) {
            String cqlStatementInsert = "INSERT INTO accelerometerdata.data " +
                "(IMEI, height, weight, age, gender, activity, timestamp, x, y, z) " +
                " VALUES ("+ cp.getImei() +
                ", " + cp.getHeight() +
                ", " + cp.getWeight() +
                ", " + cp.getAge() +
                ", " + cp.getGender() +
                ", " + cp.getActivity() +
                ", " + cp.getTimestamp() +
                ", " + cp.getX() +
                ", " + cp.getY() +
                ", " + cp.getZ() +
                ") IF NOT EXISTS;";
            session.execute(cqlStatementInsert);
    }
    
    /**
     * Indicates if the table data is empty or not
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
    public ArrayList<CompleteData> getData(int imei) {
        ArrayList<CompleteData> dataList = new ArrayList();
        String cqlStatementGet = "SELECT * FROM data WHERE IMEI = " + imei + ";";
        ResultSet rs = session.execute(cqlStatementGet);
        List<Row> listRow = rs.all();
        
        for (int i = 0; i < listRow.size(); i++){
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
     * Delete all data in the data table
     */
    @Override
    public void deleteAllData() {
        String cqlStatementDeleteAllData = "TRUNCATE accelerometerdata.data;";
        session.execute(cqlStatementDeleteAllData);
    }

    /**
     * Creates the accelerometerdata keyspace
     */
    @Override
    public void createKeyspace() {
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("system");
        
        ResultSet results = session.execute("SELECT * FROM system_schema.keyspaces " +
        "WHERE keyspace_name = 'accelerometerdata';");
        
        if (results.all().isEmpty()){
            String cqlStatementKeyspace = "CREATE KEYSPACE accelerometerdata WITH " + 
            "replication = {'class':'SimpleStrategy','replication_factor':1}";
            session.execute(cqlStatementKeyspace);
            
            session = cluster.connect("accelerometerdata");
            
            String cqlStatementTable = "CREATE TABLE data (" + 
                      " IMEI bigint, " +
                      " height int, " + 
                      " weight int, " + 
                      " age int, " + 
                      " gender int, " +
                      " activity int, " + 
                      " timestamp bigint, " + 
                      " x float, " + 
                      " y float, " +
                      " z float, " +
                      " PRIMARY KEY(IMEI, timestamp));";
            
            session.execute(cqlStatementTable);
        }
        cluster.close();
    }
    
    public boolean keyspaceExists(){
        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("system");
        
        ResultSet results = session.execute("SELECT * FROM system_schema.keyspaces " +
        "WHERE keyspace_name = 'accelerometerdata';");
        return (!results.all().isEmpty());
    }
}
