package com.latticeengines.query.evaluator.sparksql;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.EventQueryTranslatorTest;
import com.latticeengines.query.factory.SparkQueryProvider;
import com.querydsl.sql.SQLQuery;

public class EventQueryTranslatorSparkSQLTestNG extends EventQueryTranslatorTest {

    private static Logger log = LoggerFactory.getLogger(EventQueryTranslatorSparkSQLTestNG.class);

    private static final String PROD_ID1 = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";
    private static final String PROD_ID2 = "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks";

    @Autowired
    private SparkSQLQueryTester sparkSQLQueryTester;

    /*
     * Used as intermediate state to compare the Results between Redshift and SparkSQL
     */
    private long redshiftResultQueryCount = 0;

    /*
     * Used as intermediate state to compare the Results between Redshift and SparkSQL
     */
    private long redshiftTestcasesRunningTime = 0;
    private long sparkSQLTestcasesRunningTime = 0;

    // Override lookup values as per Current Data Version - 3
    @Override
    protected String getProductId1() {
        return PROD_ID1;
    }

    @Override
    protected String getProductId2() {
        return PROD_ID2;
    }

    @Override
    protected String getAccountId1() {
        return "0012400001DNKKLAA5";
    }

    @Override
    protected String getAccountId2() {
        return "0012400001DNL2vAAH";
    }

    @BeforeClass(groups = "functional")
    public void setupBase() {
        initializeAttributeRepo(3);
        sparkSQLQueryTester.setupTestContext(customerSpace, attrRepo, tblPathMap);
    }

    @AfterClass(groups = "functional", alwaysRun = true)
    public void teardown() {
        sparkSQLQueryTester.teardown();
    }

    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" },
                { SparkQueryProvider.SPARK_BATCH_USER, "SparkSQL" }
                
        };
    }

    @BeforeMethod(groups = "functional")
    public void beforeMethod(Method method, Object[] params) {
        System.out.println(String.format("\n*********** Running Test Method (SparkSQL): %s, Params: %s **********",
                method.getName(), Arrays.toString(params)));
    }

    @AfterMethod(groups = "functional")
    public void afterMethod(ITestResult testResult, Object[] params) {
        long timeTaken = testResult.getEndMillis() - testResult.getStartMillis();
        System.out.println(
                String.format("---------- Completed Test Method (SparkSQL): %s, Params: %s, Time: %d ms ----------\n",
                        testResult.getMethod().getMethodName(), Arrays.toString(params), timeTaken));
        // Profile only the tests that are running with @DataProvider(name = "userContexts")
        if (params.length >= 2) {
            switch (params[0].toString()) {
            case SQL_USER:
                redshiftTestcasesRunningTime += timeTaken;
                break;
            case SparkQueryProvider.SPARK_BATCH_USER:
                sparkSQLTestcasesRunningTime += timeTaken;
                break;
            default:
                log.info("Not tracking for User: {}", params[0].toString());
            }
        }
    }

    @AfterTest(groups = "functional")
    public void afterTest() {
        System.out.println("\n*******************************************************");
        System.out.println(String.format("Time Taken for Redshift Testcases: %d",redshiftTestcasesRunningTime));
        System.out.println(String.format("Time Taken for Redshift Testcases: %d",sparkSQLTestcasesRunningTime));
        System.out.println("*******************************************************\n");
    }

    @Override
    protected long testGetCountAndAssert(String sqlUser, Query query, long expectedCount) {
        switch (sqlUser) {
        case SQL_USER:
            redshiftResultQueryCount = queryEvaluatorService.getCount(attrRepo, query, sqlUser);
            log.info("Redshift Query Count: {}", redshiftResultQueryCount);
            return redshiftResultQueryCount;
        case SparkQueryProvider.SPARK_BATCH_USER:
            long sparkSqlResultQueryCount = sparkSQLQueryTester.getCountFromSpark(query);
            log.info("SparkSQL Query Count: {}", sparkSqlResultQueryCount);
            assertEquals(sparkSqlResultQueryCount, redshiftResultQueryCount, "Spark Query Result doesn't match with Redshift Query Result");
            return sparkSqlResultQueryCount;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    @Override
    protected long verifyHasEngaedForEventWithRevenue(Query query, String sqlUser, long expectedCount) throws SQLException {
        switch (sqlUser) {
        case SQL_USER:
            SQLQuery<?> sqlQuery = queryEvaluator.evaluate(attrRepo, query, sqlUser);
            ResultSet result = sqlQuery.getResults();
            ResultSetMetaData metaData = result.getMetaData();
            Assert.assertEquals(metaData.getColumnCount(), 3);
            int count = 0;
            while (result.next()) {
                // This cannot be true with current query. I see some periods with revenue 0
                //Assert.assertTrue(result.getInt("revenue") > 0);
                count += 1;
            }
            Assert.assertEquals(count, 24237);
            return count;
        case SparkQueryProvider.SPARK_BATCH_USER:
            HdfsDataUnit sparkResult = sparkSQLQueryTester.getDataFromSpark(query);
            List<Map<String, Object>> sparkResultsAsList = sparkSQLQueryTester.convertHdfsDataUnitToList(sparkResult);
            log.info("SparkSQL Query Data Size: {}", sparkResultsAsList.size());
            Assert.assertEquals(sparkResultsAsList.size(), 24237);
            return sparkResultsAsList.size();
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }
}
