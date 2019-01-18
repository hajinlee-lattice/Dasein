package com.latticeengines.db.service.impl.metadata;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.modeling.DbCreds;

public class MySQLServerMetadataProvider extends MetadataProvider {

    public MySQLServerMetadataProvider() {
    }

    public String getName() {
        return "MySQL";
    }

    // public String getConnectionString(DbCreds creds) {
    // String url =
    // "jdbc:mysql://$$HOST$$:$$PORT$$/$$DB$$?user=$$USER$$&password=$$PASSWD$$";
    // return replaceUrlWithParamsAndTestConnection(url, getDriverClass(),
    // creds);
    // }

    @Override
    public Long getRowCount(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap(String.format("select count(*) from `%s`", tableName));
        return (Long) resMap.get("count(*)");
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap(String.format("show table status where name = '%s'",
                tableName));
        BigInteger dataSize = (BigInteger) resMap.get("Data_length");
        return dataSize.longValue();
    }

    @Override
    public String getDriverName() {
        return "MySQL Connector Java";
    }

    @Override
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        jdbcTemplate.execute(String.format("create table `%s` select * from `%s`", newTableName, oldTableName));
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName) {
        jdbcTemplate.execute(String.format("create table `%s` select * from `%s` where 1 = 0", newTableName,
                oldTableName));
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.execute(String.format("drop table if exists `%s`", tableName));
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String tableName) {
        return jdbcTemplate.queryForList(String.format("show tables like '%s'", tableName), String.class);
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String tableName, String pid) {
        if (!checkIfColumnExists(jdbcTemplate, tableName, pid)) {
            jdbcTemplate.execute(String
                    .format("alter table `%s` add %s INT PRIMARY KEY auto_increment", tableName, pid));
        }
    }

    @Override
    public String getDriverClass() {
        return "com.mysql.jdbc.Driver";
    }

    @Override
    public String getJdbcUrlTemplate() {
        return "jdbc:mysql://$$HOST$$:$$PORT$$/$$DB$$?user=$$USER$$&password=$$PASSWD$$";
    }

    @Override
    public String getConnectionUrl(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringBefore(completeUrl, "?user=");
    }

    @Override
    public String getConnectionUserName(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringBetween(completeUrl, "user=", "&password=");
    }

    @Override
    public String getConnectionPassword(DbCreds creds) {
        String completeUrl = getConnectionString(creds);
        return StringUtils.substringAfter(completeUrl, "password=");
    }

    @Override
    public String getConnectionUrl(String completeUrl) {
        if (completeUrl.contains("useSSL=true")) {
            completeUrl += "&useSSL=true";
        }
        return StringUtils.substringBefore(completeUrl, "?user=");
    }

    @Override
    public String getConnectionUserName(String completeUrl) {
        return StringUtils.substringBetween(completeUrl, "user=", "&password=");
    }

    @Override
    public String getConnectionPassword(String completeUrl) {
        return StringUtils.substringAfter(completeUrl, "password=");
    }

    @Override
    public Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName) {
        Integer positiveEventCount = jdbcTemplate.queryForObject( //
                String.format("SELECT COUNT(*) FROM `%s` WHERE %s = 1", tableName, eventColName), //
                Integer.class);
        return Long.valueOf(positiveEventCount);
    }

    @Override
    public void createNewTable(JdbcTemplate jdbcTemplate, String tableName, String columnInfo) {
        jdbcTemplate.execute(String.format("create table `%s` " + columnInfo, tableName));
    }

    @Override
    public int insertRow(JdbcTemplate jdbcTemplate, String tableName, String columnStatement, Object... args) {
        return jdbcTemplate.update(String.format("insert into `%s` " + columnStatement, tableName), args);
    }

    @Override
    public boolean checkIfColumnExists(JdbcTemplate jdbcTemplate, String tableName, String column) {
        Integer count = jdbcTemplate.queryForObject(String.format(
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_NAME = '%s' and COLUMN_NAME='%s'",
                tableName, column), Integer.class);
        if (count > 0) {
            return true;
        }
        return false;
    }

    @Override
    public List<String> getDistinctColumnValues(JdbcTemplate jdbcTemplate, String tableName, String column) {
        return jdbcTemplate.queryForList(String.format("SELECT DISTINCT `%s` FROM `%s`", column, tableName),
                String.class);
    }
}
