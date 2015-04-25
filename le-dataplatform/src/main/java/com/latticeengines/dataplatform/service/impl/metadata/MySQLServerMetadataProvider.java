package com.latticeengines.dataplatform.service.impl.metadata;

import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.manager.MySQLManager;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
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
        String driverClass = "com.mysql.jdbc.Driver";
        try {
            Class.forName(driverClass);
        } catch (ClassNotFoundException e) {
            throw new LedpException(LedpCode.LEDP_11000, e,
                    new String[] { driverClass });
        }
        return replaceUrlWithParamsAndTestConnection(url, creds);
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
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable){
        jdbcTemplate.execute("create table " + newTable + " select * from " + oldTable + " where 1 = 0");
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String table) {
        jdbcTemplate.execute("drop table if exists " + table);
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String table){
        return jdbcTemplate.queryForList("show tables like '" + table + "'", String.class);
    }
}
