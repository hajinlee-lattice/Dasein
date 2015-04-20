package com.latticeengines.dataplatform.exposed.service;

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

    String createNewEmptyTableFromExistingOne(JdbcTemplate jdbcTemplate, String newTable, String oldTable);

    String dropTable(JdbcTemplate jdbcTemplate, String table);
    
}
