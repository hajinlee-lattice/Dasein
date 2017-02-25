package com.latticeengines.redshiftdb.load;

import java.util.Random;

import org.springframework.jdbc.core.JdbcTemplate;

public class QueryLoadTest extends AbstractLoadTest {

    public QueryLoadTest(int testerNum, JdbcTemplate jdbcTemplate, int noOfTestsPerThread) {
        super(testerNum, jdbcTemplate, noOfTestsPerThread);
        // pick a random select query
        sqlStatementType = LoadTestStatementType.values()[new Random().nextInt(2)];
    }

    @Override
    public void executeStatement() {
        redshiftJdbcTemplate.queryForObject(getSqlStatementFromType(sqlStatementType), Integer.class);
    }
}
