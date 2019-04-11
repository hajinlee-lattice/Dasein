package com.latticeengines.query.evaluator.sparksql;

import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.avro.Schema.Field;
import org.apache.avro.util.Utf8;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.ITestResult;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.Query;
import com.latticeengines.query.evaluator.QueryRunnerTestNG;
import com.latticeengines.query.factory.SparkQueryProvider;

public class QueryRunnerSparkSQLTestNG extends QueryRunnerTestNG {
    private static Logger log = LoggerFactory.getLogger(QueryRunnerSparkSQLTestNG.class);

    private static final String BITENCODED_NOMINAL_ATTR = "TechIndicator_EmailCampaigns";

    @Override
    protected String getBitEncodedNominalAttr() {
        return BITENCODED_NOMINAL_ATTR;
    }

    @Autowired
    private SparkSQLQueryTester sparkSQLQueryTester;

    /*
     * Used as intermediate state to compare the Results between Redshift and SparkSQL
     * As same test case is calling the count() api multiple times, storing one latest count would not be suffice.
     * We need to store sequence of results in test case and compare Between different Environment contexts
     */
    private List<Long> redshiftQueryCountResults = new ArrayList<>();
    private List<Long> sparkQueryCountResults = new ArrayList<>();

    private List<List<Map<String, Object>>> redshiftQueryDataResults = new ArrayList<>();
    private List<List<Map<String, Object>>> sparkQueryDataResults = new ArrayList<>();

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
                { SPARK_BATCH_USER, "SparkSQL" }
        };
    }

    @DataProvider(name = "bitEncodedData", parallel = false)
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

    @BeforeMethod(groups = "functional")
    public void beforeMethod(Method method, Object[] params) {
        System.out.println(String.format("\n*********** Running Test Method (SparkSQL): %s, Params: %s **********",
                method.getName(), Arrays.deepToString(params)));
    }

    @AfterMethod(groups = "functional")
    public void afterMethod(ITestResult testResult, Object[] params) {
        long timeTaken = testResult.getEndMillis() - testResult.getStartMillis();
        String currUserContext = String.valueOf(params[0]);
        try {
            switch (currUserContext) {
            case SQL_USER:
                log.info("Redshift Query Count Collection: {}", redshiftQueryCountResults);
                log.info("Redshift Query Data Collection Size: {}",
                        redshiftQueryDataResults.stream().map(lst -> lst.size()).collect(Collectors.toList()));
                break;
            case SPARK_BATCH_USER:
                log.info("SparkSQL Query Count Collection: {}", sparkQueryCountResults);
                log.info("Spark Query Data Collection Size: {}",
                        sparkQueryDataResults.stream().map(lst -> lst.size()).collect(Collectors.toList()));
                assertTrue(redshiftQueryCountResults.equals(sparkQueryCountResults),
                        String.format("Counts doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                                Arrays.deepToString(params)));
                assertTrue(redshiftQueryDataResults.equals(sparkQueryDataResults),
                        String.format("Data doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                                Arrays.deepToString(params)));
                break;
            }
        } finally {
            System.out.println(String.format(
                    "---------- Completed Test Method (SparkSQL): %s, Params: %s, Time: %d ms ----------\n",
                    testResult.getMethod().getMethodName(), Arrays.deepToString(params), timeTaken));
            if (SPARK_BATCH_USER.equalsIgnoreCase(currUserContext)) {
                // We Need to reset these counts only when SparkSQLTest is run. Because
                // @AfterMethod gets triggered for each user context
                redshiftQueryCountResults = new ArrayList<>();
                sparkQueryCountResults = new ArrayList<>();
                
                redshiftQueryDataResults = new ArrayList<>();
                sparkQueryDataResults = new ArrayList<>();
            }
        }
    }

    @Override
    protected long testGetCountAndAssert(String sqlUser, Query query, long expectedCount) {
        switch (sqlUser) {
        case SQL_USER:
            long redshiftQueryCount = queryEvaluatorService.getCount(attrRepo, query, sqlUser);
            log.info("Redshift Query Count: {}", redshiftQueryCount);
            redshiftQueryCountResults.add(redshiftQueryCount);
            return redshiftQueryCount;
        case SparkQueryProvider.SPARK_BATCH_USER:
            long sparkQueryCount = sparkSQLQueryTester.getCountFromSpark(query);
            log.info("SparkSQL Query Count: {}", sparkQueryCount);
            sparkQueryCountResults.add(sparkQueryCount);
            return sparkQueryCount;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    @Override
    protected List<Map<String, Object>> testGetDataAndAssert(String sqlUser, Query query, long expectedCount, List<Map<String, Object>> expectedResult) {
        switch (sqlUser) {
        case SQL_USER:
            List<Map<String, Object>> redshiftResults = queryEvaluatorService.getData(attrRepo, query, sqlUser).getData();
            log.info("Redshift Query Data Size: {}", redshiftResults.size());
            redshiftQueryDataResults.add(redshiftResults);
            return redshiftResults;
        case SparkQueryProvider.SPARK_BATCH_USER:
            HdfsDataUnit sparkResult = sparkSQLQueryTester.getDataFromSpark(query);
            List<Map<String, Object>> sparkResultsAsList = convertHdfsDataUnitToList(sparkResult);
            log.info("SparkSQL Query Data Size: {}", sparkResultsAsList.size());
            sparkQueryDataResults.add(sparkResultsAsList);
            return sparkResultsAsList;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    protected List<Map<String, Object>> convertHdfsDataUnitToList(HdfsDataUnit sparkResult) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        String avroPath = sparkResult.getPath();
        AvroUtils.AvroFilesIterator iterator = AvroUtils.avroFileIterator(yarnConfiguration, avroPath + "/*.avro");
        iterator.forEachRemaining(record -> {
            Map<String, Object> row = new HashMap<>();
            for (Field field: record.getSchema().getFields()) {
                Object value = record.get(field.name());
                if (value != null && value instanceof Utf8) {
                    value = ((Utf8)value).toString();
                }
                row.put(field.name(), value);
            }
            resultData.add(row);
        });
        return resultData;
    }
}
