package com.latticeengines.redshiftdb.load;

import org.springframework.jdbc.core.JdbcTemplate;

public class QueryLoadTest extends AbstractLoadTest {

    public QueryLoadTest(int testerNum, int noOfTestsPerThread, int maxSleepTime, JdbcTemplate jdbcTemplate,
            LoadTestStatementType stmtType) {
        super(testerNum, noOfTestsPerThread, maxSleepTime, jdbcTemplate);

        sqlStatementType = stmtType;
    }

    @Override
    public String getSqlStatement() {
        return getSqlStatementFromType(sqlStatementType);
    }

    @Override
    public void executeStatement(String sql) {
        redshiftJdbcTemplate.queryForObject(sql, Integer.class);
    }
}
