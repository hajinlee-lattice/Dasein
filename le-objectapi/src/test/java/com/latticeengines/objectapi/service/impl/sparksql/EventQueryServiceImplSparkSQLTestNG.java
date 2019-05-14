package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.inject.Named;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;

import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.impl.EventQueryServiceImplTestNG;
import com.latticeengines.objectapi.service.sparksql.impl.EventQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;
import com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor;


@Listeners(SparkSQLTestInterceptor.class)
public class EventQueryServiceImplSparkSQLTestNG extends EventQueryServiceImplTestNG implements RedshiftAndSparkQueryObjectAPITester {

    private static Logger log = LoggerFactory.getLogger(EventQueryServiceImplSparkSQLTestNG.class);

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Inject @Named("eventQueryServiceSparkSQL")
    private EventQueryServiceSparkSQLImpl eventQueryServiceSparkSql;

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

    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setupBase() {
        isSparkSQLTest = true;

        setupTestDataWithSpark(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
        eventQueryServiceSparkSql.setLivySession(getSparkSQLQueryTester().getLivySession());
        // Init Mocks
        mockDataCollectionProxy(eventQueryServiceSparkSql.getQueryEvaluatorService());
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Override
    protected String getProductId() {
        return "eNm3Nmt72BXLZRniXJWSFO4j2jnOUpY";
    }

    @Override
    protected EventQueryService getEventQueryService(String sqlUser) {
        switch(sqlUser) {
        case SEGMENT_USER:
            return super.getEventQueryService(sqlUser);
        case SPARK_BATCH_USER:
            return eventQueryServiceSparkSql;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    @Override
    protected long testAndAssertCount(String sqlUser, long resultCount, long expectedCount) {
        return testAndAssertCountFromTester(sqlUser, resultCount, expectedCount);
    }

    @Override
    protected List<Map<String, Object>> testAndAssertData(String sqlUser, List<Map<String, Object>> results,
            List<Map<String, Object>> expectedResults) {
        return testAndAssertDataFromTester(sqlUser, results, expectedResults);
    }
}
