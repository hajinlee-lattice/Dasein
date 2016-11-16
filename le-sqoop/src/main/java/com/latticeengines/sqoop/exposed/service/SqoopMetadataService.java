package com.latticeengines.sqoop.exposed.service;

import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import org.apache.avro.Schema;

public interface SqoopMetadataService {

    DataSchema createDataSchema(DbCreds creds, String tableName);

    Schema getAvroSchema(DbCreds creds, String tableName);
}
