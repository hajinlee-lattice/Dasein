package com.latticeengines.dantedb.testframework;

import java.util.List;

import org.hibernate.Query;
import org.hibernate.SessionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;
import org.springframework.transaction.support.TransactionTemplate;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = "classpath:test-dantedb-context.xml")
public class DanteDBTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    SessionFactory sessionFactory;

    @Test(groups = "deployment")
    public void testDanteDBConnection() {
        String externalID = "DanteDBTestExtID";

        String insertTPSql = "INSERT INTO [TalkingPointCache]"
                + "           ([External_ID] ,[Play_External_ID] ,[Value] ,[Customer_ID] "
                + "             ,[Creation_Date] ,[Last_Modification_Date])"
                + "     VALUES (:externalID ,'testPlayExtID' ,'Test Talking Point' ,'LPIDanteTest'"
                + "           ,'2017-05-24 01:20:50.633' ,'2017-05-24 01:20:50.633')";

        String deleteTPSql = "DELETE FROM [TalkingPointCache]\n" + "WHERE External_ID = :externalID";

        PlatformTransactionManager ptm = applicationContext.getBean("transactionManager",
                PlatformTransactionManager.class);
        TransactionTemplate txnTemplate = new TransactionTemplate(ptm);

        // Create a Talking Point
        txnTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(TransactionStatus status) {
                Query query = sessionFactory.getCurrentSession().createSQLQuery(insertTPSql);
                query.setParameter("externalID", externalID);
                query.executeUpdate();
            }
        });

        // Select a Talking Point
        @SuppressWarnings("unchecked")
        DanteTalkingPoint dtp = (DanteTalkingPoint) txnTemplate.execute((TransactionCallback) status -> {
            Query query = sessionFactory.getCurrentSession()
                    .createQuery("FROM DanteTalkingPoint WHERE External_ID = :externalID");
            query.setParameter("externalID", externalID);
            return (DanteTalkingPoint) query.list().get(0);
        });
        Assert.assertNotNull(dtp);

        // Delete a Talking Point
        txnTemplate.execute(new TransactionCallbackWithoutResult() {
            @Override
            public void doInTransactionWithoutResult(TransactionStatus status) {
                Query query = sessionFactory.getCurrentSession().createSQLQuery(deleteTPSql);
                query.setParameter("externalID", externalID);
                query.executeUpdate();
            }
        });

        @SuppressWarnings("unchecked")
        List<DanteTalkingPoint> dtps = (List<DanteTalkingPoint>) txnTemplate.execute((TransactionCallback) status -> {
            Query query = sessionFactory.getCurrentSession()
                    .createQuery("FROM DanteTalkingPoint WHERE External_ID = :externalID");
            query.setParameter("externalID", externalID);
            return (List<DanteTalkingPoint>) query.list();
        });
        Assert.assertEquals(dtps.size(), 0);
    }

}