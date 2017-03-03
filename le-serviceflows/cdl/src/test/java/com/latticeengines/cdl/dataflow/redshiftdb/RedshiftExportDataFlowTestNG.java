package com.latticeengines.cdl.dataflow.redshiftdb;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;
import com.latticeengines.domain.exposed.dataflow.DataFlowParameters;
import com.latticeengines.domain.exposed.dataflow.flows.RedshiftDataFlowParameters;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-context.xml" })
public class RedshiftExportDataFlowTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Autowired
    private S3Service s3Service;

    @Value("${aws.test.s3.bucket}")
    private String s3Bucket;

    @Value("${common.le.stack}")
    private String leStack;

    @Test(groups = "functional")
    public void test() {
        RedshiftDataFlowParameters parameters = new RedshiftDataFlowParameters("SourceTable");
        executeDataFlow(parameters);
    }

    protected DataFlowContext createDataFlowContext(DataFlowParameters parameters) {
        String tableName = "cascadingredshifttable";
        String jsonPathPrefix = RedshiftUtils.AVRO_STAGE + "/" + leStack + "/" + tableName + "/table.jsonpath";

        try {
            Schema schema = AvroUtils.readSchemaFromLocalFile(sourcePaths.values().iterator().next());
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                RedshiftUtils.generateJsonPathsFile(schema, outputStream);
                try (ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray())) {
                    s3Service.uploadInputStream(s3Bucket, jsonPathPrefix, inputStream, true);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DataFlowContext context = super.createDataFlowContext(parameters);
        RedshiftTableConfiguration config = new RedshiftTableConfiguration();
        config.setTableName(tableName);
        config.setSortKeys(new ArrayList<>());
        config.setJsonPath("s3://" + s3Bucket + "/" + jsonPathPrefix);
        context.setProperty(RedshiftTableConfiguration.class.getSimpleName(), config);

        return context;
    }

    @Override
    protected String getFlowBeanName() {
        return "redshiftExportDataflow";
    }

    @Override
    protected String getScenarioName() {
        return "redshiftExportDataflow";
    }
}
