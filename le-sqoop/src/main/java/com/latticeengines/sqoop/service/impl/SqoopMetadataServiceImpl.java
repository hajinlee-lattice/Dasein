package com.latticeengines.sqoop.service.impl;

import java.io.IOException;
import org.apache.avro.Schema;
import org.apache.sqoop.manager.DefaultManagerFactory;
import org.apache.sqoop.orm.AvroSchemaGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.metastore.JobData;
import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.modeling.DataSchema;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.sqoop.exposed.service.SqoopMetadataService;

@SuppressWarnings("deprecation")
@Component("sqoopMetadataService")
public class SqoopMetadataServiceImpl implements SqoopMetadataService {

    @Autowired
    private DbMetadataService dbMetadataService;
    
    private AvroSchemaGenerator avroSchemaGenerator;

    @Override
    public DataSchema createDataSchema(DbCreds creds, String tableName) {
        return new DataSchema(getAvroSchema(creds, tableName));
    }

    @Override
    public Schema getAvroSchema(DbCreds dbCreds, String tableName) {
        SqoopOptions options = new SqoopOptions();
        options.setConnectString(dbMetadataService.getConnectionString(dbCreds));

        if (!StringUtils.isEmpty(dbCreds.getDriverClass())) {
            options.setDriverClassName(dbCreds.getDriverClass());
        }

        ConnManager connManager = getConnectionManager(options);
        avroSchemaGenerator = new AvroSchemaGenerator(options, connManager, tableName);
        try {
            return avroSchemaGenerator.generate();
        } catch (IOException e) {
            return null;
        }
    }
    
    public ConnManager getConnectionManager(SqoopOptions options) {
        JobData data = new JobData(options, null);
        return new DefaultManagerFactory().accept(data);
    }
}
