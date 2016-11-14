package com.latticeengines.sqoop.exposed.service;

import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import org.apache.avro.Schema;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

public interface MetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);

    Schema getAvroSchema(DbCreds creds, String tableName);

    String getJdbcConnectionUrl(DbCreds creds);

    Long getRowCount(JdbcTemplate jdbcTemplate, String tableName);

    Long getDataSize(JdbcTemplate jdbcTemplate, String tableName);

    Integer getColumnCount(JdbcTemplate jdbcTemplate, String tableName);

    Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName);

    void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName);

    void dropTable(JdbcTemplate jdbcTemplate, String tableName);

    List<String> showTable(JdbcTemplate jdbcTemplate, String tableName);

    void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String tableName, String pid);

    void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTableName, String oldTableName);

    List<String> getColumnNames(JdbcTemplate jdbcTemplate, String tableName);

    JdbcTemplate constructJdbcTemplate(DbCreds creds);

    void createNewTable(JdbcTemplate jdbcTemplate, String tableName, String columnInfo);

    int insertRow(JdbcTemplate jdbcTemplate, String tableName, String columnStatement, Object... args);

    boolean checkIfColumnExists(JdbcTemplate jdbcTemplate, String tableName, String column);

    List<String> getDistinctColumnValues(JdbcTemplate jdbcTemplate, String tableName, String column);

    String getConnectionUrl(DbCreds creds);

    String getConnectionPassword(DbCreds creds);

    String getConnectionUserName(DbCreds creds);
}
