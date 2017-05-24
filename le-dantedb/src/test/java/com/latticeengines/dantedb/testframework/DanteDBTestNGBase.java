package com.latticeengines.dantedb.testframework;

import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.transaction.annotation.Transactional;
import org.testng.annotations.Test;

import com.latticeengines.dantedb.exposed.dao.DanteTalkingPointsDao;
import com.latticeengines.domain.exposed.dantetalkingpoints.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = "classpath:test-dantedb-context.xml")
public class DanteDBTestNGBase extends AbstractTestNGSpringContextTests {

    @Autowired
    DanteTalkingPointsDao danteTalkingPointsDao;

    @Autowired
    org.springframework.orm.hibernate4.HibernateTransactionManager danteTransactionManager;

    @Test(groups = "unit")
    @Transactional
    public void create() {
        System.out.println("### START ###");
        DanteTalkingPoint dtp = new DanteTalkingPoint();
        dtp.setCustomerID("test");
        dtp.setExternalID("testExtID");
        dtp.setPlayExternalID("testPlayExtID");
        dtp.setCreationDate(new Date());
        dtp.setLastModificationDate(new Date());
        dtp.setValue("Some Talking Point");

        if (danteTalkingPointsDao == null) {
            System.out.println("dao aint workin");
            return;
        }

        danteTalkingPointsDao.create(dtp);
    }
}