package com.latticeengines.dataplatform.service.impl.metadata;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.SQLServerManager;

@SuppressWarnings("deprecation")
public class SQLServerMetadataProvider extends MetadataProvider {

    public SQLServerMetadataProvider() {
    }

    public String getName() {
        return "SQLServer";
    }

    public ConnManager getConnectionManager(SqoopOptions options) {
        return new SQLServerManager(options);
    }

    @Override
    public Long getRowCount(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap("EXEC sp_spaceused N'" + tableName + "'");
        String rowCount = (String) resMap.get("rows");
        return Long.valueOf(rowCount.trim());
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap("EXEC sp_spaceused N'" + tableName + "'");
        String dataSize = ((String) resMap.get("data")).trim().split(" ")[0];
        return 1024 * Long.valueOf(dataSize);
    }

    @Override
    public String getDriverName() {
        return "Microsoft JDBC Driver 4.0 for SQL Server";
    }

    @Override
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        jdbcTemplate.execute(String.format("SELECT * INTO [%s] FROM [%s]", newTableName, oldTableName));
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        jdbcTemplate.execute(String.format("SELECT * INTO [%s] FROM [%s] WHERE 1 = 0", newTableName, oldTableName));
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.execute(String.format("IF OBJECT_ID('%1$s', 'U') IS NOT NULL DROP TABLE [%1$s]", tableName));
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String tableName) {
        return jdbcTemplate.queryForList(String.format("SELECT [name] FROM SYS.TABLES WHERE [name] = '%s'", tableName),
                String.class);
    }

    @Override
    public String getDriverClass() {
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
    }

    @Override
    public String getJdbcUrlTemplate() {
        return "jdbc:sqlserver://$$HOST$$:$$PORT$$;databaseName=$$DB$$;user=$$USER$$;password=$$PASSWD$$";
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String tableName, String pid) {
        jdbcTemplate.execute(String.format(
                "IF COL_LENGTH('%1$s', '%2$s') IS NULL ALTER TABLE [%1$s] ADD [%2$s] INT IDENTITY", tableName, pid));
    }

    @Override
    public boolean checkIfColumnExists(JdbcTemplate jdbcTemplate, String tableName, String column) {
        List<String> columnNames = jdbcTemplate.queryForList(String.format(
                "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS where TABLE_NAME = '%1$s' AND COLUMN_NAME= '%2$s'",
                tableName, column), String.class);
        if (columnNames.size() > 0) {
            return true;
        }
        return false;
    }

    @Override
    public Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName) {
        Integer positiveEventCount = jdbcTemplate.queryForObject(
                String.format("SELECT COUNT(*) FROM [%s] WHERE [%s] = 1", tableName, eventColName), Integer.class);
        return Long.valueOf(positiveEventCount);
    }

    @Override
    public void createNewTable(JdbcTemplate jdbcTemplate, String tableName, String columnInfo) {
        jdbcTemplate.execute(String.format("create table [%s] " + columnInfo, tableName));
    }

    @Override
    public int insertRow(JdbcTemplate jdbcTemplate, String tableName, String columnStatement, Object... args) {
        return jdbcTemplate.update(String.format("insert into [%s] " + columnStatement, tableName), args);
    }

    @Override
    public List<String> getDistinctColumnValues(JdbcTemplate jdbcTemplate, String tableName, String column) {
        return jdbcTemplate.queryForList(String.format("SELECT DISTINCT [%s] FROM [%s]", column, tableName), String.class);
    }
}
