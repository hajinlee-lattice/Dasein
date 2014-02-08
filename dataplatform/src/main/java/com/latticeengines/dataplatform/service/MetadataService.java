package com.latticeengines.dataplatform.service;

import com.latticeengines.dataplatform.exposed.domain.DataSchema;
import com.latticeengines.dataplatform.exposed.domain.DbCreds;

public interface MetadataService {

	DataSchema createDataSchema(DbCreds creds, String tableName);
}
