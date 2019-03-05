package com.latticeengines.sqoop.service.impl;

import java.io.IOException;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.sqoop.ConnFactory;
import org.apache.sqoop.orm.AvroSchemaGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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

    private static final Logger log = LoggerFactory.getLogger(SqoopMetadataServiceImpl.class);

    @Inject
    private DbMetadataService dbMetadataService;

    @Inject
    private Configuration yarnConfiguration;

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

        try {
            ConnManager connManager = getConnectionManager(options);
            AvroSchemaGenerator avroSchemaGenerator = new AvroSchemaGenerator(options, connManager, tableName);
            return avroSchemaGenerator.generate();
        } catch (IOException e) {
            log.error(ExceptionUtils.getStackTrace(e));
            return null;
        }
    }

    public ConnManager getConnectionManager(SqoopOptions options) throws IOException {
        JobData data = new JobData(options, null);
        ConnManager connManager = new ConnFactory(yarnConfiguration).getManager(data);
        return connManager;
    }
}
