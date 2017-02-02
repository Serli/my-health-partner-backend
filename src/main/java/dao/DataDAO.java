/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package dao;

import java.util.ArrayList;
import service.CompleteData;

/**
 * Inteface with the database
 * @author nathan
 */

public interface DataDAO {
    public void createKeyspace();
    public void open();
    public void addEntry(CompleteData cp);
    public boolean isEmpty();
    public void close();
    public ArrayList<CompleteData> getData(int imei);
    public void deleteAllData();
}
