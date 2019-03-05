package com.latticeengines.sqoop.exposed.service;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

public interface SqoopMetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);

    Schema getAvroSchema(DbCreds creds, String tableName);
}
