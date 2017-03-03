package controller;

import dao.DataDAOImpl;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Application {

    public static void main(String[] args) {
        DataDAOImpl dao = new DataDAOImpl();
        dao.open();
        dao.createKeyspace();
        dao.close();
        SpringApplication.run(Application.class, args);
    }

}
