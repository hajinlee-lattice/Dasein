package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Arrays;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;

import com.latticeengines.objectapi.service.EntityQueryService;
import com.latticeengines.objectapi.service.impl.EntityQueryServiceImplTestNG;
import com.latticeengines.objectapi.service.sparksql.impl.EntityQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;

public class EntityQueryServiceImplSparkSQLTestNG extends EntityQueryServiceImplTestNG implements RedshiftAndSparkQueryObjectAPITester {

    private static Logger log = LoggerFactory.getLogger(EntityQueryServiceImplSparkSQLTestNG.class);

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Resource(name = "entityQueryServiceSparkSQL")
    private EntityQueryServiceSparkSQLImpl entityQueryServiceSparkSql;

    @Override
    public Logger getLogger() {
        return log;
    }

    @Override
    public SparkSQLQueryTester getSparkSQLQueryTester() {
        return sparkSQLQueryTester;
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SEGMENT_USER, "Redshift" },
                { SPARK_BATCH_USER, "Spark" }
        };
    }

    @BeforeClass(groups = "functional")
    public void setupBase() {
        setupTestDataWithSpark(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
        entityQueryServiceSparkSql.setLivySession(getSparkSQLQueryTester().getLivySession());
        // Init Mocks
        mockDataCollectionProxy(entityQueryServiceSparkSql.getQueryEvaluatorService());
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Override
    protected EntityQueryService getEntityQueryService(String sqlUser) {
        switch(sqlUser) {
        case SEGMENT_USER:
            return super.getEntityQueryService(sqlUser);
        case SPARK_BATCH_USER:
            return entityQueryServiceSparkSql;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    @DataProvider(name = "timefilterProvider", parallel = false)
    private Object[][] timefilterProvider() {
        Object[][] basicTests = super.getTimeFilterDataProvider();

        Object[][] basicTestsWithMultipleUsers = new Object[basicTests.length*2][];
        for (int i=0; i<basicTests.length; i++) {
            basicTestsWithMultipleUsers[i*2] = basicTests[i];
            // Add SparkUser for the same set of data
            Object[] sparkUserTestCase = Arrays.copyOf(basicTests[i], basicTests[i].length);
            sparkUserTestCase[0] = SPARK_BATCH_USER;
            basicTestsWithMultipleUsers[(i*2)+1] = sparkUserTestCase;
        }
        log.info("Test Data Counts from Base Class: {}, From Current Dataprovider: {}", basicTests.length,basicTestsWithMultipleUsers.length);
        return basicTestsWithMultipleUsers;
    }

    @Override
    protected long testAndAssertCount(String sqlUser, long resultCount, long expectedCount) {
        return testAndAssertCountFromTester(sqlUser, resultCount, expectedCount);
    }

//    @Override
//    protected List<Map<String, Object>> testAndAssertData(String sqlUser, List<Map<String, Object>> results,
//            List<Map<String, Object>> expectedResults) {
//        return testAndAssertDataFromTester(sqlUser, results, expectedResults);
//    }

}
