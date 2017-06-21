package com.latticeengines.cdl.workflow.steps.export;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.Properties;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.export.RedshiftPublishStepConfiguration;
import org.apache.avro.Schema;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.serviceflows.cdl.dataflow.RedshiftPublishDataFlowParameters;
import com.latticeengines.domain.exposed.metadata.JdbcStorage;
import com.latticeengines.domain.exposed.metadata.JdbcStorage.DatabaseName;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("redshiftPublishStep")
public class RedshiftPublishStep extends RunDataFlow<RedshiftPublishStepConfiguration> {

    private static final Log log = LogFactory.getLog(RedshiftPublishStep.class);

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.default.access.key.encrypted}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    @Override
    public void onConfigurationInitialized() {
        RedshiftPublishStepConfiguration configuration = getConfiguration();
        Table sourceTable = getObjectFromContext(EVENT_TABLE, Table.class);
        if (sourceTable == null) {
            sourceTable = configuration.getSourceTable();
        }
        Properties jobProperties = new Properties();
        jobProperties.setProperty("fs.s3n.awsAccessKeyId", awsAccessKey);
        jobProperties.setProperty("fs.s3n.awsSecretAccessKey", awsSecretKey);

        configuration.setTargetTableName(sourceTable.getName() + "_redshift_export");
        configuration.setDataFlowParams(new RedshiftPublishDataFlowParameters(sourceTable.getName(),
                configuration.getRedshiftTableConfiguration()));
        configuration.setSkipStep(Boolean.TRUE);
        configuration.setJobProperties(jobProperties);

        uploadJsonPathToS3(sourceTable);
    }

    private void uploadJsonPathToS3(Table eventTable) {
        Schema schema = AvroUtils.getSchemaFromGlob(yarnConfiguration, eventTable.getExtractsDirectory() + "/*.avro");
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            RedshiftUtils.generateJsonPathsFile(schema, outputStream);
            try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                Configuration config = new Configuration();
                config.set(FileSystem.FS_DEFAULT_NAME_KEY, "s3n:///");
                config.set("fs.s3n.awsAccessKeyId", awsAccessKey);
                config.set("fs.s3n.awsSecretAccessKey", awsSecretKey);
                HdfsUtils.copyInputStreamToDest(
                        new URI("s3n://" + s3Bucket + "/"
                                + configuration.getRedshiftTableConfiguration().getJsonPathPrefix()),
                        config, inputStream);
            }
        } catch (Exception e) {
            log.error(ExceptionUtils.getFullStackTrace(e));
            throw new RuntimeException(e);
        }
    }

    @Override
    public void onExecutionCompleted() {
        String bucketedTableName = configuration.getRedshiftTableConfiguration().getTableName();
        Table bucketedTable = metadataProxy.getTable(configuration.getCustomerSpace().toString(), bucketedTableName);
        JdbcStorage storage = new JdbcStorage();
        storage.setDatabaseName(DatabaseName.REDSHIFT);
        storage.setTableNameInStorage(bucketedTable.getStorageMechanism().getTableNameInStorage());
        bucketedTable.setStorageMechanism(storage);
        metadataProxy.updateTable(configuration.getCustomerSpace().toString(), bucketedTableName, bucketedTable);
        putObjectInContext(EVENT_TABLE, bucketedTable);
    }
}
