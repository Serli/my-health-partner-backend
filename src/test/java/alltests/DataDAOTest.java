/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package alltests;

import service.CompleteData;
import dao.DataDAOImpl;
import java.util.ArrayList;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 * @author nathan
 */
public class DataDAOTest{
    
    static DataDAOImpl dao;
    CompleteData cd;
    
    @BeforeClass
    public static void beforeTests(){
	dao = new DataDAOImpl();
        dao.open();
    }
    
    @Test
    public void testInsertDAO(){
        
        cd = new CompleteData();
        cd.setImei(500);
        cd.setHeight(180);
        cd.setWeight(70);
        cd.setAge(22);
        cd.setGender(1);
        
        cd.setActivity(2);
        cd.setTimestamp(646871213);
        cd.setX(3.0f);
        cd.setY(2.0f);
        cd.setZ(1.0f);
        
        dao.addEntry(cd);
    }
    
    @Test
    public void testGetUserDataDAO(){
        ArrayList<CompleteData> arrayData = dao.getData(500);
        for (int i = 0; i < arrayData.size(); i++){
            System.out.println("Données " + i + " : [" + arrayData.get(i).getActivity() + ", " +
                arrayData.get(i).getImei() + ", " +
                arrayData.get(i).getTimestamp() + ", " + 
                arrayData.get(i).getHeight() + ", " +
                arrayData.get(i).getWeight() + ", " +
                arrayData.get(i).getAge() + ", " +
                arrayData.get(i).getGender() + ", " +
                arrayData.get(i).getX() + ", " + 
                arrayData.get(i).getY() + ", " +
                arrayData.get(i).getZ() + ", " +
                arrayData.get(i).getActivity() + "]");
        }
    }
    
    @Test
    public void testDeleteAllDataDAO(){
        dao.deleteAllData();
    }
    
    @AfterClass
    public static void afterTests() throws Exception{
        dao.close();
    }
}
