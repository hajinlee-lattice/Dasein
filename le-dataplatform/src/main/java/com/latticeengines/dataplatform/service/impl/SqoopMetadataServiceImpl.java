package com.latticeengines.dataplatform.service.impl;

import org.apache.avro.Schema;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.service.SqoopMetadataService;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;

@Lazy
@Deprecated
@Component("sqoopMetadataService")
public class SqoopMetadataServiceImpl implements SqoopMetadataService {

    @Override
    public DataSchema createDataSchema(DbCreds creds, String tableName) {
        return new DataSchema(getAvroSchema(creds, tableName));
    }

    @Override
    public Schema getAvroSchema(DbCreds dbCreds, String tableName) {
        // TODO: remove all LP2 code
        return null;
    }

}
