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
        Map<String, Object> resMap = jdbcTemplate.queryForMap("select count(*) from " + tableName);
        return (Long) resMap.get("count(*)");
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        Map<String, Object> resMap = jdbcTemplate.queryForMap("show table status where name = '" + tableName + "'");
        BigInteger dataSize = (BigInteger) resMap.get("Data_length");
        return dataSize.longValue();
    }

    @Override
    public String getDriverName() {
        return "MySQL Connector Java";
    }

    @Override
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        jdbcTemplate.execute("create table " + newTable + " select * from " + oldTable);
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        jdbcTemplate.execute("create table " + newTable + " select * from " + oldTable + " where 1 = 0");
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String table) {
        jdbcTemplate.execute("drop table if exists " + table);
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String table) {
        return jdbcTemplate.queryForList("show tables like '" + table + "'", String.class);
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String table, String pid) {
        jdbcTemplate.execute("alter table " + table + " add " + pid + " INT PRIMARY KEY auto_increment");
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
                String.format("SELECT COUNT(*) FROM %s WHERE %s = 1", tableName, eventColName), //
                Integer.class);
        return Long.valueOf(positiveEventCount);
    }

}
