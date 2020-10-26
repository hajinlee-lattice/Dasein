package com.latticeengines.query.evaluator.athena;

import static com.latticeengines.query.factory.AthenaQueryProvider.ATHENA_USER;
import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;
import static org.testng.Assert.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.testng.ITestResult;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
public interface RedshiftAndAthenaQueryTester {

    Logger getLogger();
    AthenaQueryTester getQueryTester();

    default long getCountFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    default DataPage getDataFromRedshift(AttributeRepository attrRepo, Query query, String sqlUser) {
        throw new UnsupportedOperationException("Implement the method in TestClass");
    }

    List<Long> redshiftQueryCountResults = new ArrayList<>();
    List<Long> prestoQueryCountResults = new ArrayList<>();

    List<List<Map<String, Object>>> redshiftQueryDataResults = new ArrayList<>();
    List<List<Map<String, Object>>> prestoQueryDataResults = new ArrayList<>();

    default void setupQueryTester(CustomerSpace customerSpace, AttributeRepository attrRepo) {
        getQueryTester().setupTestContext(customerSpace, attrRepo);
    }

    default void teardownQueryTester() {
    }

    @BeforeMethod(groups = "functional")
    default void beforeMethod(Method method, Object[] params) {
        getLogger().info(
                "\n*********** Running Test Method (Redshift-Athena): {}}, Params: {} **********%n",
                method.getName(), Arrays.deepToString(params));
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
                        redshiftQueryDataResults.stream().map(List::size).collect(Collectors.toList()));
                break;
            case ATHENA_USER:
                getLogger().info("Athena Query Count Collection: {}", prestoQueryCountResults);
                getLogger().info("Athena Query Data Collection Size: {}",
                        prestoQueryDataResults.stream().map(List::size).collect(Collectors.toList()));
                assertEquals(redshiftQueryCountResults, prestoQueryCountResults,
                        String.format("Counts doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                                Arrays.deepToString(params)));
                assertEquals(CollectionUtils.size(prestoQueryDataResults), CollectionUtils.size(redshiftQueryDataResults));
                for (int i = 0; i < CollectionUtils.size(prestoQueryDataResults); i++) {
                    assertEquals(prestoQueryDataResults.get(i), redshiftQueryDataResults.get(i),
                            String.format("Counts doesn't match for %s : %s", testResult.getMethod().getMethodName(),
                            Arrays.deepToString(params)));
                }
                break;
            default:
            }
        } finally {
            getLogger().info(
                    "---------- Completed Test Method (Redshift-Athena): {}, Params: {}, Time: {} ms ----------\n%n",
                    testResult.getMethod().getMethodName(), Arrays.deepToString(params), timeTaken);
            if (ATHENA_USER.equalsIgnoreCase(currUserContext) || StringUtils.isBlank(currUserContext)) {
                // We Need to reset these counts only when SparkSQLTest is run. Because
                // @AfterMethod gets triggered for each user context
                redshiftQueryCountResults.clear();
                prestoQueryCountResults.clear();

                redshiftQueryDataResults.clear();
                prestoQueryDataResults.clear();
            }
        }
    }

    default long testGetCountAndAssertFromTester(String sqlUser, Query query, long expectedCount) {
        switch (sqlUser) {
        case USER_SEGMENT:
            long redshiftQueryCount = getCountFromRedshift(getQueryTester().getAttrRepo(), query, sqlUser);
            getLogger().info("Redshift Query Count: {}", redshiftQueryCount);
            redshiftQueryCountResults.add(redshiftQueryCount);
            return redshiftQueryCount;
        case ATHENA_USER:
            long prestoQueryCount = getQueryTester().getCountFromAthena(query);
            getLogger().info("Athena Query Count: {}", prestoQueryCount);
            prestoQueryCountResults.add(prestoQueryCount);
            return prestoQueryCount;
        default:
            throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
        }
    }

    default List<Map<String, Object>> testGetDataAndAssertFromTester(String sqlUser, Query query, //
                                                 long expectedCount, List<Map<String, Object>> expectedResult) {
        switch (sqlUser) {
        case USER_SEGMENT:
            List<Map<String, Object>> redshiftResults = getDataFromRedshift(getQueryTester().getAttrRepo(), query, sqlUser).getData();
            getLogger().info("Redshift Query Data Size: {}", redshiftResults.size());
            redshiftQueryDataResults.add(redshiftResults);
            return redshiftResults;
        case ATHENA_USER:
            List<Map<String, Object>> prestoResultsAsList = getQueryTester().getDataFromAthena(query).getData();
            getLogger().info("Athena Query Data Size: {}", prestoResultsAsList.size());
            prestoQueryDataResults.add(prestoResultsAsList);
            return prestoResultsAsList;
        default:
            throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
        }
    }
}
