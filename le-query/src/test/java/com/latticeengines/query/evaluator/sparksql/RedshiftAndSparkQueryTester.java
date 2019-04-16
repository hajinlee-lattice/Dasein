package com.latticeengines.query.evaluator.sparksql;

import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;

public interface RedshiftAndSparkQueryTester {

    public Logger getLogger();
    public SparkSQLQueryTester getSparkSQLQueryTester();

    public default long getCountFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    public default DataPage getDataFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    public List<Long> redshiftQueryCountResults = new ArrayList<>();
    public List<Long> sparkQueryCountResults = new ArrayList<>();

    public List<List<Map<String, Object>>> redshiftQueryDataResults = new ArrayList<>();
    public List<List<Map<String, Object>>> sparkQueryDataResults = new ArrayList<>();

    public default void setupQueryTester(CustomerSpace customerSpace, AttributeRepository attrRepo, Map<String, String> tblPathMap) {
        getSparkSQLQueryTester().setupTestContext(customerSpace, attrRepo, tblPathMap);
    }

    public default void teardownQueryTester() {
        getSparkSQLQueryTester().teardown();
    }

    @BeforeMethod(groups = "functional")
    public default void beforeMethod(Method method, Object[] params) {
        System.out.println(String.format("\n*********** Running Test Method (Redshift-SparkSQL): %s, Params: %s **********",
                method.getName(), Arrays.deepToString(params)));
    }

    @AfterMethod(groups = "functional")
    public default void afterMethod(ITestResult testResult, Object[] params) {
        long timeTaken = testResult.getEndMillis() - testResult.getStartMillis();
        String currUserContext = String.valueOf(params[0]);
        try {
            switch (currUserContext) {
            case USER_SEGMENT:
                getLogger().info("Redshift Query Count Collection: {}", redshiftQueryCountResults);
                getLogger().info("Redshift Query Data Collection Size: {}",
                        redshiftQueryDataResults.stream().map(lst -> lst.size()).collect(Collectors.toList()));
                break;
            case SPARK_BATCH_USER:
                getLogger().info("SparkSQL Query Count Collection: {}", sparkQueryCountResults);
                getLogger().info("Spark Query Data Collection Size: {}",
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
                    "---------- Completed Test Method (Redshift-SparkSQL): %s, Params: %s, Time: %d ms ----------\n",
                    testResult.getMethod().getMethodName(), Arrays.deepToString(params), timeTaken));
            if (SPARK_BATCH_USER.equalsIgnoreCase(currUserContext)) {
                // We Need to reset these counts only when SparkSQLTest is run. Because
                // @AfterMethod gets triggered for each user context
                redshiftQueryCountResults.clear();
                sparkQueryCountResults.clear();
                
                redshiftQueryDataResults.clear();
                sparkQueryDataResults.clear();
            }
        }
    }

    public default long testGetCountAndAssertFromTester(String sqlUser, Query query, long expectedCount) {
        switch (sqlUser) {
        case USER_SEGMENT:
            long redshiftQueryCount = getCountFromRedshift(getSparkSQLQueryTester().getAttrRepo(), query, sqlUser);
            getLogger().info("Redshift Query Count: {}", redshiftQueryCount);
            redshiftQueryCountResults.add(redshiftQueryCount);
            return redshiftQueryCount;
        case SPARK_BATCH_USER:
            long sparkQueryCount = getSparkSQLQueryTester().getCountFromSpark(query);
            getLogger().info("SparkSQL Query Count: {}", sparkQueryCount);
            sparkQueryCountResults.add(sparkQueryCount);
            return sparkQueryCount;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    public default List<Map<String, Object>> testGetDataAndAssertFromTester(String sqlUser, Query query, long expectedCount, List<Map<String, Object>> expectedResult) {
        switch (sqlUser) {
        case USER_SEGMENT:
            List<Map<String, Object>> redshiftResults = getDataFromRedshift(getSparkSQLQueryTester().getAttrRepo(), query, sqlUser).getData();
            getLogger().info("Redshift Query Data Size: {}", redshiftResults.size());
            redshiftQueryDataResults.add(redshiftResults);
            return redshiftResults;
        case SPARK_BATCH_USER:
            HdfsDataUnit sparkResult = getSparkSQLQueryTester().getDataFromSpark(query);
            List<Map<String, Object>> sparkResultsAsList = getSparkSQLQueryTester().convertHdfsDataUnitToList(sparkResult);
            getLogger().info("SparkSQL Query Data Size: {}", sparkResultsAsList.size());
            sparkQueryDataResults.add(sparkResultsAsList);
            return sparkResultsAsList;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }
}
