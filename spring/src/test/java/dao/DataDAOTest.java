package dao;

import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.util.Assert;

public class DataDAOTest {

    static DataDAOImpl dao;
    CompleteData cd;

    @BeforeClass
    public static void beforeTests() {
        dao = new DataDAOImpl();
        dao.open();
        Assert.notNull(dao);
        dao.createKeyspace();
        Assert.isTrue(dao.keyspaceExists());
    }

    @Before
    public void testInsertDAO() {

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

        dao.addDataEntry(cd);
        Assert.isTrue(!dao.isEmpty());
    }

    @Test
    public void testGetUserDataDAO() {
        List<CompleteData> arrayData = dao.getData(500);
        for (int i = 0; i < arrayData.size(); i++) {
            System.out.println("Data " + i + " : [" + arrayData.get(i).getActivity() + ", " +
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

    @After
    public void testDeleteDataDAO() {
        dao.deleteData(500);
        Assert.isTrue(dao.getData(500).isEmpty());
    }

    @AfterClass
    public static void afterTests() throws Exception {
        dao.close();
    }
}