package com.latticeengines.objectapi.service.impl.sparksql;

import static com.latticeengines.query.factory.RedshiftQueryProvider.USER_SEGMENT;
import static com.latticeengines.query.factory.SparkQueryProvider.SPARK_BATCH_USER;

import java.util.List;
import java.util.Map;

import com.latticeengines.query.evaluator.sparksql.RedshiftAndSparkQueryTester;

public interface RedshiftAndSparkQueryObjectAPITester extends RedshiftAndSparkQueryTester {

    public default long testAndAssertCountFromTester(String sqlUser, long resultCount, long expectedCount) {
        switch (sqlUser) {
        case USER_SEGMENT:
            getLogger().info("Redshift Query Count: {}", resultCount);
            redshiftQueryCountResults.add(resultCount);
            return resultCount;
        case SPARK_BATCH_USER:
            getLogger().info("SparkSQL Query Count: {}", resultCount);
            sparkQueryCountResults.add(resultCount);
            return resultCount;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }

    public default List<Map<String, Object>> testAndAssertDataFromTester(String sqlUser, List<Map<String, Object>> results,
            List<Map<String, Object>> expectedResults) {
        switch (sqlUser) {
        case USER_SEGMENT:
            getLogger().info("Redshift Query Data Size: {}", results.size());
            redshiftQueryDataResults.add(results);
            return results;
        case SPARK_BATCH_USER:
            getLogger().info("SparkSQL Query Data Size: {}", results.size());
            sparkQueryDataResults.add(results);
            return results;
        }
        throw new IllegalArgumentException(String.format("SQL User: %s is not supported", sqlUser));
    }
}
