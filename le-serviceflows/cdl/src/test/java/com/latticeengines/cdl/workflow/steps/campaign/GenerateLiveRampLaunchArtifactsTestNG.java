package com.latticeengines.cdl.workflow.steps.campaign;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.cdl.channel.MediaMathChannelConfig;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.GenerateLiveRampLaunchArtifactStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.workflow.functionalframework.WorkflowTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-cdl-workflow-context.xml",
        "classpath:test-serviceflows-cdl-context.xml" })
public class GenerateLiveRampLaunchArtifactsTestNG extends WorkflowTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(GenerateLiveRampLaunchArtifactsTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    @Spy
    private GenerateLiveRampLaunchArtifacts generateLiveRampLaunchArtifacts;

    private GenerateLiveRampLaunchArtifactStepConfiguration configuration;

    private ExecutionContext executionContext;
    private CustomerSpace customerSpace;
    private String addedAccountsLocation;
    private String removedAccountsLocation;

    private static final String[] TEST_JOB_LEVELS = { "Software Engineer", "CEO", "President" };
    private static final String[] TEST_JOB_FUNCTIONS = { "Marketing", "Research", "HR" };
    private static final String MOCKED_ADD_ACCOUNTS_TABLE = "ADD_DELTA_TABLE_LOCATION";
    private static final String MOCKED_REMOVE_ACCOUNTS_TABLE = "REMOVE_DELTA_TABLE_LOCATION";
    private static final String FAKE_EXECUTION_ID = "1234321";
    private static final String CREATED_ADD_ACCOUNTS_TABLE_NAME = String.format("AddedContacts_%s", FAKE_EXECUTION_ID);
    private static final String CREATED_REMOVE_ACCOUNTS_TABLE_NAME = String.format(
            "RemovedContacts_%s",
            FAKE_EXECUTION_ID);

    @Override
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        cleanupBeforeTests();
        setUpAvroFiles();

        MediaMathChannelConfig testChannelConfig = new MediaMathChannelConfig();
        testChannelConfig.setJobFunctions(TEST_JOB_FUNCTIONS);
        testChannelConfig.setJobLevels(TEST_JOB_LEVELS);

        customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);

        configuration = new GenerateLiveRampLaunchArtifactStepConfiguration();
        configuration.setCustomerSpace(customerSpace);
        configuration.setExecutionId(FAKE_EXECUTION_ID);
        generateLiveRampLaunchArtifacts.setConfiguration(configuration);

        MockitoAnnotations.initMocks(this);

        Mockito.doReturn(testChannelConfig).when(generateLiveRampLaunchArtifacts).getChannelConfig(any());

        Mockito.doReturn(MOCKED_ADD_ACCOUNTS_TABLE)
                .when(
                generateLiveRampLaunchArtifacts)
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.ADDED_ACCOUNTS_DELTA_TABLE);
        Mockito.doReturn(
                MOCKED_REMOVE_ACCOUNTS_TABLE)
                .when(generateLiveRampLaunchArtifacts)
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_ACCOUNTS_DELTA_TABLE);

        Mockito.doReturn(addedAccountsLocation).when(generateLiveRampLaunchArtifacts)
                .getAvroPathFromTable(MOCKED_ADD_ACCOUNTS_TABLE);
        Mockito.doReturn(removedAccountsLocation).when(generateLiveRampLaunchArtifacts)
                .getAvroPathFromTable(MOCKED_REMOVE_ACCOUNTS_TABLE);

    }

    @Test(groups = "functional")
    public void testBulkMathTps() {
        executionContext = new ExecutionContext();
        generateLiveRampLaunchArtifacts.setExecutionContext(executionContext);

        generateLiveRampLaunchArtifacts.execute();

        String addedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE);
        String removedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE);

        assertEquals(CREATED_ADD_ACCOUNTS_TABLE_NAME, addedContactsDeltaTable);
        assertEquals(CREATED_REMOVE_ACCOUNTS_TABLE_NAME, removedContactsDeltaTable);

        Table addTable = metadataProxy.getTable(customerSpace.getTenantId(), addedContactsDeltaTable);
        Table removeTable = metadataProxy.getTable(customerSpace.getTenantId(), removedContactsDeltaTable);
        assertNotNull(addTable);
        assertNotNull(removeTable);
        log.info(JsonUtils.serialize(addTable));
        log.info(JsonUtils.serialize(removeTable));
    }

    @AfterClass(groups = "functional")
    public void cleanup() {
        String addedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.ADDED_CONTACTS_DELTA_TABLE);
        String removedContactsDeltaTable = generateLiveRampLaunchArtifacts
                .getStringValueFromContext(GenerateLiveRampLaunchArtifacts.REMOVED_CONTACTS_DELTA_TABLE);

        cleanupFromCustomerSpace(addedContactsDeltaTable);
        cleanupFromCustomerSpace(removedContactsDeltaTable);
    }

    public void cleanupBeforeTests() {
        String addedContactsDeltaTable = CREATED_ADD_ACCOUNTS_TABLE_NAME;
        String removedContactsDeltaTable = CREATED_REMOVE_ACCOUNTS_TABLE_NAME;
        cleanupFromCustomerSpace(addedContactsDeltaTable);
        cleanupFromCustomerSpace(removedContactsDeltaTable);
    }

    public void cleanupFromCustomerSpace(String tableName) {
        try {
            Table table = metadataProxy.getTable(customerSpace.getTenantId(), tableName);

            if (table != null) {
                HdfsDataUnit dataUnit = table.toHdfsDataUnit(tableName);
                log.info("Fetched dataunit for cleanup " + dataUnit.getPath());

                metadataProxy.deleteTable(customerSpace.getTenantId(), tableName);
                log.info("Removed table " + tableName);

                HdfsUtils.rmdir(yarnConfiguration, dataUnit.getPath());
                log.info("Removed dataunit at " + dataUnit.getPath());
            }
        } catch (Exception e) {
            log.error(e.toString());
        }
    }

    private void setUpAvroFiles() throws Exception {
        Schema deltaAccountSchema = SchemaBuilder.record("Account").fields() //
                .name("DUNS").type().longType()
                .noDefault()//
                .endRecord();

        String fileName = "addedAccounts";
        addedAccountsLocation = createAvroFromJson(fileName,
                String.format("com/latticeengines/cdl/workflow/campaign/%s.json", fileName), deltaAccountSchema,
                DeltaAccount.class, yarnConfiguration);
        log.info("Positive Delta Accounts uploaded to: " + addedAccountsLocation);

        fileName = "removedAccounts";
        removedAccountsLocation = createAvroFromJson(fileName,
                String.format("com/latticeengines/cdl/workflow/campaign/%s.json", fileName), deltaAccountSchema,
                DeltaAccount.class, yarnConfiguration);
        log.info("Negative Delta Accounts uploaded to: " + removedAccountsLocation);
    }

    static class DeltaAccount implements AvroExportable {
        public Long getSiteDuns() {
            return siteDuns;
        }

        public void setDuns(Long siteDuns) {
            this.siteDuns = siteDuns;
        }

        @JsonProperty(value = "LDC_DUNS")
        private Long siteDuns;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("LDC_DUNS", this.getSiteDuns());
            return builder.build();
        }

    }

    interface AvroExportable {
        GenericRecord getAsRecord(Schema schema);
    }

    @SuppressWarnings("unchecked")
    private static <T extends AvroExportable> String createAvroFromJson(String fileName, String jsonPath,
            Schema schema,
            Class<T> elementClazz, Configuration yarnConfiguration) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream tableRegistryStream = classLoader.getResourceAsStream(jsonPath);
        String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
        List<T> accounts = JsonUtils.convertList(raw, elementClazz);
        String extension = ".avro";
        String avroPath = "/tmp/campaign/" + fileName + extension;

        File localFile = new File(avroPath);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, localFile);
            accounts.forEach(account -> {
                try {
                    dataFileWriter.append(account.getAsRecord(schema));
                } catch (IOException ioe) {
                    log.warn("failed to write a avro datdum", ioe);
                }
            });
            dataFileWriter.flush();
        }
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroPath);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath));

        return avroPath;
    }
}
