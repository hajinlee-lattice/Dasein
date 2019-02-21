package com.latticeengines.db.service.impl.metadata;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.modeling.DbCreds;

public class SQLServerMetadataProvider extends MetadataProvider {

    public SQLServerMetadataProvider() {
    }

    public String getName() {
        return "SQLServer";
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
        return "Microsoft JDBC Driver 7.0 for SQL Server";
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

    public String getConnectionUrl(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringBefore(completeUrl, ";user=");
    }

    public String getConnectionUserName(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringBetween(completeUrl, "user=", ";password=");
    }

    public String getConnectionPassword(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringAfter(completeUrl, "password=");
    }

    @Override
    public String getConnectionUrl(String completeUrl) {
        return StringUtils.substringBefore(completeUrl, ";user=");
    }

    @Override
    public String getConnectionUserName(String completeUrl) {
        return StringUtils.substringBetween(completeUrl, "user=", ";password=");
    }

    @Override
    public String getConnectionPassword(String completeUrl) {
        return StringUtils.substringAfter(completeUrl, "password=");
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
        return jdbcTemplate.queryForList(String.format("SELECT DISTINCT [%s] FROM [%s]", column, tableName),
                String.class);
    }
}
