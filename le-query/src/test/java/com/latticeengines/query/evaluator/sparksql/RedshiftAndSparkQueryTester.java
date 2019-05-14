package com.latticeengines.query.evaluator.sparksql;

import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.statistics.AttributeRepository;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.Query;

/**
 * @author jadusumalli
 *
 * Created this test interface to check the results between Redshift and Spark.
 * Goal of this interface is to use existing testcases as is, without modifying the expected outputs.
 * Because existing testcases are using version 1 of data set in Redshift.
 * This interface allows us to test with any dataset, as we donot assert for any specific number.
 *  Instead it just stored the results from Redshift testcase and compares with Spark testcase results.
 *
 * This allows us test new usecases with different data sets versions. without hard coding expected outputs.
 *
 */
public interface RedshiftAndSparkQueryTester {

    Logger getLogger();
    SparkSQLQueryTester getSparkSQLQueryTester();

    default long getCountFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    default DataPage getDataFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    List<Long> redshiftQueryCountResults = new ArrayList<>();
    List<Long> sparkQueryCountResults = new ArrayList<>();

    List<List<Map<String, Object>>> redshiftQueryDataResults = new ArrayList<>();
    List<List<Map<String, Object>>> sparkQueryDataResults = new ArrayList<>();

    default void setupQueryTester(CustomerSpace customerSpace, AttributeRepository attrRepo, Map<String, String> tblPathMap) {
        getSparkSQLQueryTester().setupTestContext(customerSpace, attrRepo, tblPathMap);
    }

    default void teardownQueryTester() {
        getSparkSQLQueryTester().teardown();
    }

    @BeforeMethod(groups = "functional")
    default void beforeMethod(Method method, Object[] params) {
        System.out.println(String.format("\n*********** Running Test Method (Redshift-SparkSQL): %s, Params: %s **********",
                method.getName(), Arrays.deepToString(params)));
    }

    @AfterMethod(groups = "functional")
    default void afterMethod(ITestResult testResult, Object[] params) {
        long timeTaken = testResult.getEndMillis() - testResult.getStartMillis();
        // There could be some methods in base class, which doesn't support DataProvider implementation.
        // Those methods will not have any arguments
        String currUserContext = params.length > 1 ? String.valueOf(params[0]) : "";
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
                assertEquals(redshiftQueryCountResults, sparkQueryCountResults,
                        String.format("Counts doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                                Arrays.deepToString(params)));
                assertEquals(redshiftQueryDataResults, sparkQueryDataResults,
                        String.format("Data doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                                Arrays.deepToString(params)));
                break;
            }
        } finally {
            System.out.println(String.format(
                    "---------- Completed Test Method (Redshift-SparkSQL): %s, Params: %s, Time: %d ms ----------\n",
                    testResult.getMethod().getMethodName(), Arrays.deepToString(params), timeTaken));
            if (SPARK_BATCH_USER.equalsIgnoreCase(currUserContext) || StringUtils.isBlank(currUserContext)) {
                // We Need to reset these counts only when SparkSQLTest is run. Because
                // @AfterMethod gets triggered for each user context
                redshiftQueryCountResults.clear();
                sparkQueryCountResults.clear();

                redshiftQueryDataResults.clear();
                sparkQueryDataResults.clear();
            }
        }
    }

    default long testGetCountAndAssertFromTester(String sqlUser, Query query, long expectedCount) {
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
        default:
            throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
        }
    }

    default List<Map<String, Object>> testGetDataAndAssertFromTester(String sqlUser, Query query, long expectedCount, List<Map<String, Object>> expectedResult) {
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
        default:
            throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
        }
    }
}
