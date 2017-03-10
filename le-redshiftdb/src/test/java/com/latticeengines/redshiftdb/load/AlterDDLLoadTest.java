package com.latticeengines.redshiftdb.load;

import org.springframework.jdbc.core.JdbcTemplate;

public class AlterDDLLoadTest extends AbstractLoadTest {

    private String testColumnName;

    public AlterDDLLoadTest(int testerNum, int noOfTestsPerThread, int maxSleepTime, JdbcTemplate jdbcTemplate) {
        super(testerNum, noOfTestsPerThread, maxSleepTime, jdbcTemplate);
        sqlStatementType = null;
        // ensure every tester uses their own column
        testColumnName = "load_test_column_" + testerID;
    }

    @Override
    public String getSqlStatement() {
        // Alternate between ADD and DROP
        if (sqlStatementType == null || sqlStatementType == LoadTestStatementType.DDL_Drop)
            sqlStatementType = LoadTestStatementType.DDL_Add;
        else
            sqlStatementType = LoadTestStatementType.DDL_Drop;
        String ddlStmt = getSqlStatementFromType(sqlStatementType);
        return ddlStmt.replaceAll("load_test_column", testColumnName);
    }

    @Override
    protected void executeStatement(String sql) {

        redshiftJdbcTemplate.execute(sql);
    }

    @Override
    protected void cleanup() {
        // Drop test column in case Add was the last executed statement
        if (sqlStatementType == LoadTestStatementType.DDL_Add)
            redshiftJdbcTemplate.execute(getSqlStatementFromType(LoadTestStatementType.DDL_Drop)
                    .replaceAll("load_test_column", testColumnName));
    }
}
