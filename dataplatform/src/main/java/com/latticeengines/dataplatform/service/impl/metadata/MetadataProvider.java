package com.latticeengines.dataplatform.service.impl.metadata;

import org.apache.avro.Schema;

import com.latticeengines.dataplatform.exposed.domain.DbCreds;

public interface MetadataProvider {

    String getName();

    String getConnectionString(DbCreds creds);

    Schema getSchema(DbCreds dbCreds, String tableName);
}
