package com.latticeengines.dataplatform.service.impl.metadata;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.MySQLManager;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@SuppressWarnings("deprecation")
public class MySQLServerMetadataProvider extends MetadataProvider {

    public MySQLServerMetadataProvider() {
    }

    public String getName() {
        return "MySQL";
    }

    public String getConnectionString(DbCreds creds) {
        String url = "jdbc:mysql://$$HOST$$:$$PORT$$/$$DB$$?user=$$USER$$&password=$$PASSWD$$";
        return replaceUrlWithParamsAndTestConnection(url, getDriverClass(), creds);
    }

    public ConnManager getConnectionManager(SqoopOptions options) {
        return new MySQLManager(options);
    }

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
        jdbcTemplate.execute(String.format("create table `%s` select * from `%s` where 1 = 0", newTableName, oldTableName));
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
        jdbcTemplate.execute(String.format("alter table `%s` add %s INT PRIMARY KEY auto_increment", tableName, pid));
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

}
