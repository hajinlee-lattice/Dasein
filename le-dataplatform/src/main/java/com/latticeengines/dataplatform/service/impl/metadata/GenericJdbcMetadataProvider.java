package com.latticeengines.dataplatform.service.impl.metadata;

import java.util.List;

import org.apache.sqoop.manager.GenericJdbcManager;
import org.springframework.jdbc.core.JdbcTemplate;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;

@SuppressWarnings("deprecation")
public class GenericJdbcMetadataProvider extends MetadataProvider {

    @Override
    public String getName() {
        return "GenericJDBC";
    }

    @Override
    public String getDriverName() {
        return "Generic JDBC Driver";
    }

    @Override
    public ConnManager getConnectionManager(SqoopOptions options) {
        return new GenericJdbcManager(options.getDriverClassName(), options);
    }

    @Override
    public Long getRowCount(JdbcTemplate jdbcTemplate, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getDataSize(JdbcTemplate jdbcTemplate, String tableName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropTable(JdbcTemplate jdbcTemplate, String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<String> showTable(JdbcTemplate jdbcTemplate, String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String table, String pid) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getDriverClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getJdbcUrlTemplate() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void createNewTable(JdbcTemplate jdbcTemplate, String table, String columnInfo) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int insertRow(JdbcTemplate jdbcTemplate, String table, String columnStatement, Object... args) {
        throw new UnsupportedOperationException();
    }

}
