package com.latticeengines.cdl.dataflow.redshiftdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataflow.exposed.builder.common.DataFlowProperty;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.RedshiftPublishDataFlowParameters;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.DistStyle;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration.SortKeyType;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-dataflow-context.xml" })
public class RedshiftPublishDataFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${aws.default.access.key.encrypted}")
    private String awsAccessKey;

    @Value("${aws.default.secret.key.encrypted}")
    private String awsSecretKey;

    private String tableName = "cascadingredshifttable";

    @Test(groups = "functional")
    public void test() {
        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setTableName(tableName);
        redshiftTableConfig.setSortKeys(new ArrayList<>());
        String jsonPathPrefix = RedshiftUtils.AVRO_STAGE + "/" + leStack + "/" + tableName + "/table.jsonpath";
        redshiftTableConfig.setJsonPathPrefix("s3://" + s3Bucket + "/" + jsonPathPrefix);
        redshiftTableConfig.setDistStyle(DistStyle.Key);
        redshiftTableConfig.setDistKey("lastmodifieddate");
        redshiftTableConfig.setSortKeyType(SortKeyType.Compound);
        redshiftTableConfig.setSortKeys(Arrays.asList(new String[] { "id" }));
        RedshiftPublishDataFlowParameters parameters = new RedshiftPublishDataFlowParameters("SourceTable",
                redshiftTableConfig);
        executeDataFlow(parameters);
    }

    protected DataFlowContext createDataFlowContext(DataFlowParameters parameters) {

        try {
            Schema schema = AvroUtils.readSchemaFromLocalFile(sourcePaths.values().iterator().next());
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                RedshiftUtils.generateJsonPathsFile(schema, outputStream);
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                    Configuration config = new Configuration();
                    config.set(FileSystem.FS_DEFAULT_NAME_KEY, "s3n:///");
                    config.set("fs.s3n.awsAccessKeyId", awsAccessKey);
                    config.set("fs.s3n.awsSecretAccessKey", awsSecretKey);
                    HdfsUtils.copyInputStreamToDest(new URI("s3n://" + s3Bucket + "/"
                            + ((RedshiftPublishDataFlowParameters) parameters).redshiftTableConfig.getJsonPathPrefix()),
                            config, inputStream);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DataFlowContext context = super.createDataFlowContext(parameters);
        Properties jobProperties = new Properties();
        jobProperties.setProperty("fs.s3n.awsAccessKeyId", awsAccessKey);
        jobProperties.setProperty("fs.s3n.awsSecretAccessKey", awsSecretKey);
        context.setProperty(DataFlowProperty.JOBPROPERTIES, jobProperties);
        context.setProperty(DataFlowProperty.PARTITIONS, 2);
        return context;
    }

    @Override
    protected String getFlowBeanName() {
        return "redshiftPublishDataflow";
    }

    @Override
    protected String getScenarioName() {
        return "redshiftPublishDataflow";
    }
}
