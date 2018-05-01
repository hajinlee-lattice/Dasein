package com.latticeengines.workflowapi.steps.core;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.flows.testflows.testdynamo.TestDynamoWorkflowConfiguration;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class ExportToDynamoDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    public static final String TABLE_1 = "Table1";
    public static final String TABLE_2 = "Table2";

    private static final int NUM_ACCOUNTS = 10;
    private static final int NUM_CONTACTS_PER_ACCOUNT = 2;

    @Inject
    private WorkflowService workflowService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Value("${eai.export.dynamo.signature}")
    private String signature;

    @Override
    @BeforeClass(groups = "deployment" )
    public void setup() throws Exception {
        super.setup();
        prepareTables();
    }

    @Test(groups = "deployment")
    public void testExport() throws Exception {
        TestDynamoWorkflowConfiguration workflowConfig = generateConfiguration();
        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS, 1000).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    private TestDynamoWorkflowConfiguration generateConfiguration() {
        TestDynamoWorkflowConfiguration.Builder builder = new TestDynamoWorkflowConfiguration.Builder();
        return builder //
                .internalResourceHostPort(internalResourceHostPort) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(mainTestCustomerSpace) //
                .dynamoSignature(signature) //
                .build();
    }

    private void prepareTables() throws Exception {
        prepareTable1();
        prepareTable2();
    }

    private void prepareTable1() throws IOException {
        List<Pair<String, Class<?>>> columns = Arrays.asList(
                Pair.of("AccountId", String.class),
                Pair.of("AccountName", String.class),
                Pair.of("CreatedTime", Long.class)
        );
        Schema schema = AvroUtils.constructSchema(TABLE_1, columns);
        List<GenericRecord> records = new ArrayList<>();
        long createdTime = System.currentTimeMillis();
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", String.format("ACC%04d", i));
            builder.set("AccountName", UUID.randomUUID().toString());
            builder.set("CreatedTime", createdTime);
            records.add(builder.build());
        }
        String podId = CamilleEnvironment.getPodId();
        String extractPath = PathBuilder.buildDataTablePath(podId, mainTestCustomerSpace).append(TABLE_1).toString();
        String avroPath = extractPath + "/part-00000.avro";
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, avroPath, records);

        Table table = MetadataConverter.getTable(yarnConfiguration, extractPath, "AccountId", null);
        metadataProxy.createTable(mainTestCustomerSpace.toString(), TABLE_1, table);
    }

    private void prepareTable2() throws IOException {
        List<Pair<String, Class<?>>> columns = Arrays.asList(
                Pair.of("ContactId", String.class),
                Pair.of("AccountId", String.class),
                Pair.of("FirstName", String.class),
                Pair.of("SecondName", String.class),
                Pair.of("CreatedTime", Long.class)
        );
        Schema schema = AvroUtils.constructSchema(TABLE_2, columns);
        List<GenericRecord> records = new ArrayList<>();
        long createdTime = System.currentTimeMillis();
        for (int i = 0; i < NUM_ACCOUNTS; i++) {
            for (int j = 0; j < NUM_CONTACTS_PER_ACCOUNT; j++) {
                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
                builder.set("AccountId", String.format("ACC%04d", i));
                builder.set("ContactId", String.format("CNT%04d%02d", i, j));
                String name = UUID.randomUUID().toString();
                builder.set("FirstName", name.substring(0, 10));
                builder.set("SecondName", name.substring(10));
                builder.set("CreatedTime", createdTime);
                records.add(builder.build());
            }
        }
        String podId = CamilleEnvironment.getPodId();
        String extractPath = PathBuilder.buildDataTablePath(podId, mainTestCustomerSpace).append(TABLE_2).toString();
        String avroPath = extractPath + "/part-00000.avro";
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, avroPath, records);

        Table table = MetadataConverter.getTable(yarnConfiguration, extractPath);
        metadataProxy.createTable(mainTestCustomerSpace.toString(), TABLE_1, table);
    }
}
