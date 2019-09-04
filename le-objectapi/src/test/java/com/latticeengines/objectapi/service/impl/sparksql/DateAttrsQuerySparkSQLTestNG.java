package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.EventType;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.objectapi.service.EventQueryService;
import com.latticeengines.objectapi.service.impl.QueryServiceImplTestNGBase;
import com.latticeengines.objectapi.service.sparksql.impl.EventQueryServiceSparkSQLImpl;
import com.latticeengines.query.evaluator.sparksql.SparkSQLQueryTester;

public class DateAttrsQuerySparkSQLTestNG extends QueryServiceImplTestNGBase
    implements RedshiftAndSparkQueryObjectAPITester {

    private static final Logger log = LoggerFactory.getLogger(DateAttrsQuerySparkSQLTestNG.class);

    @Inject
    private SparkSQLQueryTester sparkSQLQueryTester;

    @Resource(name = "eventQueryServiceSparkSQL")
    private EventQueryService eventQueryServiceSparkSQL;

    @Inject
    private EventQueryService eventQueryService;

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
        return new Object[][] { { SEGMENT_USER, "Redshift" }, { SPARK_BATCH_USER, "Spark" } };
    }

    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setupBase() {
        setupTestDataWithSpark(3);
        sparkSQLQueryTester.setupTestContext(customerSpace, attrRepo, tblPathMap);
        ((EventQueryServiceSparkSQLImpl) eventQueryServiceSparkSQL).setLivySession(sparkSQLQueryTester.getLivySession());
        // Init Mocks
        mockDataCollectionProxy(
                ((EventQueryServiceSparkSQLImpl) eventQueryServiceSparkSQL).getQueryEvaluatorService());
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @Test(groups = SPARK_TEST_GROUP, dataProvider = "userContexts")
    public void testLatestDayOperator(String sqlUser, String queryContext) {
        String fileName = "vmware.json";
        EventFrontEndQuery frontEndQuery = loadEventFrontEndQueryFromResource(fileName);
        frontEndQuery.getSegmentQuery().setEvaluationDateStr(maxTransactionDate);
        long count;
        if (SPARK_BATCH_USER.equals(sqlUser)) {
            String sql = eventQueryService.getQueryStr(frontEndQuery, EventType.Scoring, //
                    DataCollection.Version.Blue);
            System.out.println("========== " + fileName + " ==========");
            System.out.println(sql);
            System.out.println("========== " + fileName + " ==========");
            count = eventQueryServiceSparkSQL.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
        } else {
            count = eventQueryService.getScoringCount(frontEndQuery, DataCollection.Version.Blue);
        }
        testAndAssertCountFromTester(sqlUser, count, 0L);
    }

}
