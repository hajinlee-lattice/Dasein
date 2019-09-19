package com.latticeengines.query.evaluator.sparksql;

import static com.latticeengines.query.evaluator.sparksql.SparkSQLTestInterceptor.SPARK_TEST_GROUP;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;

import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryRunnerTestNG;

@Listeners(SparkSQLTestInterceptor.class)
public class QueryRunnerSparkSQLTestNG extends QueryRunnerTestNG implements RedshiftAndSparkQueryTester {

    private static Logger log = LoggerFactory.getLogger(QueryRunnerSparkSQLTestNG.class);

    private static final String BITENCODED_NOMINAL_ATTR = "TechIndicator_EmailCampaigns";

    @Override
    protected String getBitEncodedNominalAttr() {
        return BITENCODED_NOMINAL_ATTR;
    }

    @Autowired
    private SparkSQLQueryTester sparkSQLQueryTester;

    @BeforeClass(groups = SPARK_TEST_GROUP)
    public void setupBase() {
        initializeAttributeRepo(3);
        setupQueryTester(customerSpace, attrRepo, tblPathMap);
    }

    @AfterClass(groups = SPARK_TEST_GROUP, alwaysRun = true)
    public void teardown() {
        teardownQueryTester();
    }

    @BeforeMethod(groups = SPARK_TEST_GROUP)
    public void refreshSparkSession() {
        if (sparkSQLQueryTester.getCurrentSessionUsage() > 10) {
            sparkSQLQueryTester.refreshLivySession();
        }
    }

    @DataProvider(name = "userContexts")
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" },
                { SPARK_BATCH_USER, "SparkSQL" }
        };
    }

    @DataProvider(name = "bitEncodedData")
    private Object[][] provideBitEncodedDataWithSparkUser() {
        Object[][] basicTests = super.getBitEncodedTestData();

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
    public Logger getLogger() {
        return log;
    }

    @Override
    public SparkSQLQueryTester getSparkSQLQueryTester() {
        return sparkSQLQueryTester;
    }

    @Override
    protected long testGetCountAndAssert(String sqlUser, Query query, long expectedCount) {
        return testGetCountAndAssertFromTester(sqlUser, query, expectedCount);
    }

    @Override
    protected List<Map<String, Object>> testGetDataAndAssert(String sqlUser, Query query, long expectedCount, List<Map<String, Object>> expectedResult) {
        return testGetDataAndAssertFromTester(sqlUser, query, expectedCount, expectedResult);
    }

    @Override
    public long getCountFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        return queryEvaluatorService.getCount(attrRepo, query, sqlUser);
    }

    @Override
    public DataPage getDataFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        return queryEvaluatorService.getData(attrRepo, query, sqlUser);
    }

}
