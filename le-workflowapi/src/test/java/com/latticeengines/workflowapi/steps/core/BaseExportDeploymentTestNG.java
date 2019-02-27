package com.latticeengines.workflowapi.steps.core;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.exposed.service.WorkflowService;
import com.latticeengines.workflowapi.functionalframework.WorkflowFrameworkDeploymentTestNGBase;

public abstract class BaseExportDeploymentTestNG<T extends WorkflowConfiguration>
        extends WorkflowFrameworkDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(BaseExportDeploymentTestNG.class);

    public static final String TABLE_1 = "Table1";
    public static final String TABLE_2 = "Table2";
    public static final String TABLE_3 = "Table3";

    private static final int NUM_ACCOUNTS = 10;
    private static final int NUM_UPDATED_ACCOUNTS = 5;
    protected static final int NUM_CONTACTS_PER_ACCOUNT = 2;

    @Inject
    protected WorkflowService workflowService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    protected String tenantName;

    protected abstract T generateCreateConfiguration();
    protected abstract T generateUpdateConfiguration();
    protected abstract void verifyCreateExport();
    protected abstract void verifyUpdateExport();

    @Override
    @BeforeClass(groups = "deployment" )
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
        prepareTables();
        tenantName = mainTestCustomerSpace.getTenantId();
    }

    @Test(groups = "deployment")
    public void testExport() throws Exception {
        T workflowConfig = generateCreateConfiguration();
        runWorkflow(workflowConfig);
        verifyCreateExport();

        workflowService.unRegisterJob(workflowConfig.getName());

        workflowConfig = generateUpdateConfiguration();
        runWorkflow(workflowConfig);
        verifyUpdateExport();
    }

    private void prepareTables() throws Exception {
        prepareTable1();
        prepareTable2();
        prepareTable3();
    }

    private void prepareTable1() throws IOException {
        prepareAccountTable(TABLE_1, "ACC1-%s", NUM_ACCOUNTS);
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

    private void prepareTable3() throws IOException {
        prepareAccountTable(TABLE_3, "ACC2-%s", NUM_UPDATED_ACCOUNTS);
    }

    private void prepareAccountTable(String tableName, String accountNamePattern, int numAccounts) throws IOException {
        List<Pair<String, Class<?>>> columns = Arrays.asList(
                Pair.of("AccountId", String.class),
                Pair.of("AccountName", String.class),
                Pair.of("CreatedTime", Long.class)
        );
        Schema schema = AvroUtils.constructSchema(tableName, columns);
        List<GenericRecord> records = new ArrayList<>();
        long createdTime = System.currentTimeMillis();
        for (int i = 0; i < numAccounts; i++) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", String.format("ACC%04d", i));
            builder.set("AccountName", String.format(accountNamePattern, UUID.randomUUID().toString()));
            builder.set("CreatedTime", createdTime);
            records.add(builder.build());
        }
        String podId = CamilleEnvironment.getPodId();
        String extractPath = PathBuilder.buildDataTablePath(podId, mainTestCustomerSpace).append(tableName).toString();
        String avroPath = extractPath + "/part-00000.avro";
        AvroUtils.writeToHdfsFile(yarnConfiguration, schema, avroPath, records);

        Table table = MetadataConverter.getTable(yarnConfiguration, extractPath, "AccountId", null);
        metadataProxy.createTable(mainTestCustomerSpace.toString(), tableName, table);
    }

}
