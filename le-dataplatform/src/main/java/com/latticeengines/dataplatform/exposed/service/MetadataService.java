package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.avro.Schema;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public interface MetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);

    Schema getAvroSchema(DbCreds creds, String tableName);

    String getJdbcConnectionUrl(DbCreds creds);

    Long getRowCount(JdbcTemplate jdbcTemplate, String tableName);

    Long getDataSize(JdbcTemplate jdbcTemplate, String tableName);

    Integer getColumnCount(JdbcTemplate jdbcTemplate, String tableName);

    Long getPositiveEventCount(JdbcTemplate jdbcTemplate, String tableName, String eventColName);

    void createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable);

    void dropTable(JdbcTemplate jdbcTemplate, String table);

    List<String> showTable(JdbcTemplate jdbcTemplate, String table);

    void addPrimaryKeyColumn(JdbcTemplate jdbcTemplate, String table, String pid);

    void createNewTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable);

    List<String> getColumnNames(JdbcTemplate jdbcTemplate, String table);

    JdbcTemplate constructJdbcTemplate(DbCreds creds);
}
