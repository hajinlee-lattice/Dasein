package com.latticeengines.dataplatform.service;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public interface MetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);
    
    Schema getAvroSchema(DbCreds creds, String tableName);

    String getJdbcConnectionUrl(DbCreds creds);
}
