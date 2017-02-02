/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller;

import dao.DataDAOImpl;
import java.util.ArrayList;
import javax.validation.Valid;
import service.CompleteData;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller which receives data sent by the developer application
 * Then, it stores it in the learning database
 * @author kawther D
 */

@RestController
@RequestMapping("/data")
public class DataController {
   
    private final DataDAOImpl dao = new DataDAOImpl();
    
    @RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    public void insertData(@RequestBody @Valid ArrayList<CompleteData> completeData) {
        dao.open();
        for (int i = 0; i < completeData.size(); i++){
            dao.addEntry(completeData.get(i));
        }
        dao.close();
    }
}
