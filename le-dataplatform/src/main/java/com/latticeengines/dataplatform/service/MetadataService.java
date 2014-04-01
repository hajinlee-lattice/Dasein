package com.latticeengines.dataplatform.service;

import org.apache.avro.Schema;

import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;

public interface MetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);
    
    Schema getAvroSchema(DbCreds creds, String tableName);
}
