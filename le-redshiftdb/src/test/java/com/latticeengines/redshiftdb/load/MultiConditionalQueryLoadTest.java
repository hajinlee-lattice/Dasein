package com.latticeengines.redshiftdb.load;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.springframework.jdbc.core.JdbcTemplate;

public class MultiConditionalQueryLoadTest extends AbstractLoadTest {

    private List<Column> columns;

    private class Column {
        Column(String cName, String cType) {
            columnName = cName;
            columnType = cType;

        }

        public String getColumnName() {
            return columnName;
        }

        public String getColumnType() {
            return columnType;
        }

        private String columnName;
        private String columnType;
    }

    public MultiConditionalQueryLoadTest(int testerNum, int noOfTestsPerThread, int maxSleepTime,
            JdbcTemplate jdbcTemplate) {
        super(testerNum, noOfTestsPerThread, maxSleepTime, jdbcTemplate);
        columns = populateIntegerColumns();
        sqlStatementType = LoadTestStatementType.Query_Multi_Conditional;
    }

    private List<Column> populateIntegerColumns() {
        String findNumericAndBoolColumnsSql = "select \"column\" as columnName, type as columnType"
                + " from pg_table_def " //
                + "where tablename = 'loadtesteventtable2' " //
                + "and type in ('bigint','smallint', 'double precision', 'real')";

        return redshiftJdbcTemplate.query(findNumericAndBoolColumnsSql, (rs, rowNum) -> {
            return new Column(rs.getString(1), rs.getString(2));
        });
    }

    @Override
    public String getSqlStatement() {
        String sql = getSqlStatementFromType(sqlStatementType);
        Random rng = new Random();
        int noOfClauses = rng.nextInt(15 - 1) + 1;

        for (int i = 0; i < noOfClauses; ++i) {
            Column toAdd = columns.get(rng.nextInt(columns.size()));
            sql += String.format(" A.%s %s %s OR", toAdd.getColumnName(),
                    getRandomComparisonType(toAdd.getColumnType()), getValueforType(toAdd.getColumnType()));
        }

        sql = sql.substring(0, sql.length() - 3);
        return sql;
    }

    @Override
    public void executeStatement(String sql) {
        redshiftJdbcTemplate.queryForObject(sql, Integer.class);
    }

    private String getValueforType(String columnType) {
        Random rng = new Random();
        if (columnType.equals("boolean")) {
            return Integer.toString(rng.nextInt(2));
        }
        return Integer.toString(rng.nextInt(100));
    }

    private String getRandomComparisonType(String columnType) {
        List<String> comparisonTypes = Arrays.asList("=", "!=", "<", "<=", ">", ">=");
        Random rng = new Random();
        if (columnType.equals("boolean")) {
            return comparisonTypes.get(rng.nextInt(2));
        }
        return comparisonTypes.get(rng.nextInt(comparisonTypes.size()));
    }
}
