package com.latticeengines.redshiftdb.load;

import org.springframework.jdbc.core.JdbcTemplate;

public class QueryLoadTest extends AbstractLoadTest {

    public QueryLoadTest(int testerNum, int noOfTestsPerThread, int maxSleepTime, JdbcTemplate jdbcTemplate) {
        super(testerNum, noOfTestsPerThread, maxSleepTime, jdbcTemplate);
        // pick a random select query
        sqlStatementType = LoadTestStatementType.Query_Numeric;
    }

    @Override
    public void executeStatement() {
        redshiftJdbcTemplate.queryForObject(getSqlStatementFromType(sqlStatementType), Integer.class);
    }
}
