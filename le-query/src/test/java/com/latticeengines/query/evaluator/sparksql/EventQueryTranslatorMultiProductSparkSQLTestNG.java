package com.latticeengines.query.evaluator.sparksql;

import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.EventQueryTranslatorMultiProductTest;
import com.latticeengines.query.exposed.translator.EventQueryTranslator;
import com.latticeengines.query.factory.SparkQueryProvider;

public class EventQueryTranslatorMultiProductSparkSQLTestNG extends EventQueryTranslatorMultiProductTest {

    private static Logger log = LoggerFactory.getLogger(EventQueryTranslatorMultiProductSparkSQLTestNG.class);

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

    private List<String> productIds = Arrays.asList(
            "Hd3c9G01LBGBF7LKRGu9Oc0dS8HnuRdv", // Product 0
            "DkhSLVHpu5iIS5jw4aWOmojNlz7CvAa", // Product 1
            "eNm3Nmt72BXLZRniXJWSFO4j2jnOUpY", // Product 2
            "H8u0TkdKoeqD2b8bEFNwcWagzKmPJk", // Product 3
            "o13brsbfF10fllM6VUZRxMO7wfo5I7Ks", // Product 4 
            "uKt9Tnd4sTXNUxEMzvIXcC9eSkaGah8", // Product 5
            "k4Pb7AhrIccPjW5jXiWzpwWX2mTvY7I", // Product 6
            "LHdgKhusYLJqk9JgC5SdJrwNv8tg9il", // Product 7
            "yhJKT0T3Pu64VprFieVLFbstNw17yz1h", // Product 8
            "zqpVPoOPufEuPkcxqjDFMykEHN7ocY", // Product 9
            "uiUrfXNLCGpDIkTAB5he5iLbhGwLik0I", // Product 10 
            "hpf28LuxIoQzJ4gy4u20Vwazew0t", // Product 11
            "5Bg2lBhLITlRpSMIMv8qtVMRS9ortPL", // Product 12
            "hLxxPy0B6A4ehW7FaaGRh9LAjNzsYicB", // Product 13
            "uIbuNRuc30AVD8Hqz0r3bJoscKyrZoJj", // Product 14
            "aBbvvgqeFEGCr9h1JZpGP9NZDsRerBhC", // Product 15
            "TyZ3prYakrgeUdVTxbu7UPNAF3GtN8", // Product 16
            "FcUOE0yZRyjId7f9MZt2WDTWo6wHXRo", // Product 17
            "I0VN2ZbBuKtXeQs8LFPOqwBxRgjpoLq", // Product 18
            "1ydd4TfF8tNf1yeAzi4EnrrYWrBOAa", // Product 19
            "iGfEB6bqSCdrYXsd17BmIU5FK1Wd7A0I", // Product 20
            "Bl2ObAv3Smmm0xLD1bXMYnQ0zs4Hsnh8" // Product 21
    );

    @Override
    public List<String> getProductIds() {
        return productIds;
    }

    protected EventQueryTranslator getEventQueryTranslator() {
        return new EventQueryTranslator();
    }

    @Override
    protected int getDefaultPeriodId() {
        return 198;
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
    
    @DataProvider(name = "userContexts", parallel = false)
    private Object[][] provideSqlUserContexts() {
        return new Object[][] {
                { SQL_USER, "Redshift" },
                { SparkQueryProvider.SPARK_BATCH_USER, "SparkSQL" }
                
        };
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

}
