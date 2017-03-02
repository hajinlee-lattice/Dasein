package com.latticeengines.redshiftdb.load;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.jdbc.core.JdbcTemplate;

public class Tester extends AbstractLoadTest {
    private static final Log log = LogFactory.getLog(Tester.class);

    private final List<LoadTestStatementType> statements;
    private int index = 0;

    public Tester(int i, List<LoadTestStatementType> statements, int maxSleepTime, JdbcTemplate redshiftJdbcTemplate) {
        super(i, statements.size(), maxSleepTime, redshiftJdbcTemplate);
        this.statements = statements;
    }

    @Override
    protected void executeStatement() {
        LoadTestStatementType statementType = statements.get(index++);
        try {
            String sql = getSqlStatementFromType(statementType);
            DateTime start = DateTime.now();
            redshiftJdbcTemplate.execute(sql);
            DateTime end = DateTime.now();
            recordTime(statementType, end.getMillis() - start.getMillis());
        } catch (Exception e) {
            log.warn(String.format("Failed to execute statement %s: %s", statementType, e.getMessage()));
        }
    }
}
