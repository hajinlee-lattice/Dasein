package com.latticeengines.spark.exposed.job.cdl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.file.FileReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateLaunchUniverseJobTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "generateLaunchUniverse";
    }

    @Override
    protected String getScenarioName() {
        return "contactsPerAccount";
    }

    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchUniverseJobTestNG.class);

    private static final String CDL_UPDATED_TIME = InterfaceName.CDLUpdatedTime.name();
    private static final String DESC = "DESC";
    private static final String AVRO_EXTENSION = ".avro";

    private static final HdfsFileFilter avroFileFilter = new HdfsFileFilter() {
        @Override
        public boolean accept(FileStatus file) {
            return file.getPath().getName().endsWith("avro");
        }
    };

    @Inject
    private Configuration yarnConfiguration;

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobContactLimit");

        config.setMaxContactsPerAccount(2L);
        config.setMaxEntitiesToLaunch(20L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobContactLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobNoLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobNoLimit");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobNoLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 20);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobBothLimits() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobBothLimits");

        config.setMaxContactsPerAccount(2L);
        config.setMaxEntitiesToLaunch(13L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobBothLimits Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 13);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobColumnNotFound() throws Exception {
        // When sort column is not found, use ContactId to sort
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobColumnNotFound");

        config.setMaxContactsPerAccount(2L);
        config.setContactsPerAccountSortAttribute("Unknown Column");
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobColumnNotFound Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobDefaultSort() throws Exception {
        // When sort column is not found, use ContactId to sort
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobDefaultSort");
        config.setMaxContactsPerAccount(2L);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobDefaultSort Results: " + JsonUtils.serialize(result));

        Assert.assertTrue(verifyDefaultSort(result));
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitNotApplied() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitNotApplied");

        config.setContactAccountRatioThreshold(2L);
        config.setMaxContactsPerAccount(2L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobThresholdLimitNotApplied Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitApplied() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitApplied");
        config.setContactAccountRatioThreshold(2L);

        log.info("Config: " + JsonUtils.serialize(config));
        String error = null;

        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
            log.info("TestGenerateLaunchUniverseJobThresholdLimitApplied Results: " + JsonUtils.serialize(result));
        } catch (Exception e) {
            error = e.getMessage();
        }

        Assert.assertNotNull(error);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobThresholdLimitApplied2() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobThresholdLimitApplied2");
        config.setContactAccountRatioThreshold(2L);
        config.setMaxContactsPerAccount(3L);

        log.info("Config: " + JsonUtils.serialize(config));
        String error = null;

        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
            log.info("TestGenerateLaunchUniverseJobThresholdLimitApplied2 Results: " + JsonUtils.serialize(result));
        } catch (Exception e) {
            error = e.getMessage();
        }

        Assert.assertNotNull(error);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactsData() throws Exception {
        Schema contactSchema = SchemaBuilder.record("Contact").fields() //
                .name("ContactId").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("AccountId").type().stringType().noDefault() //
                .endRecord();

        String fileName = "limitedContacts";
        createAvroFromJson(fileName,
                String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                contactSchema, Contact.class, yarnConfiguration);
        DataUnit contactsData = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchUniverse" + fileName + AVRO_EXTENSION);
        logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchUniverse" + fileName));

        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobContactsData");
        config.setContactAccountRatioThreshold(10L);
        config.setContactsData(contactsData);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobContactsData Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 20);
    }

    private boolean verifyDefaultSort(SparkJobResult result) throws Exception {
        String hdfsDir = result.getTargets().get(0).getPath();
        String fieldName = "ContactId";
        String ACCOUNT_ID = "AccountId";
        List<String> avroFilePaths = HdfsUtils.getFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);
        Map<String, List<String>> expectedIdsMap = getExpectedIdsMap();
        int index = 0;

        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();
            log.info("File path is: " + filePathStr);
    
            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String contactId = getString(record, fieldName);
                    String accountId = getString(record, ACCOUNT_ID);
                    List<String> expectedIds = expectedIdsMap.get(accountId);
                    log.info("contactId: " + contactId + " / accountId: " + accountId);
                    if (!expectedIds.get(index).equals(contactId)) {
                        log.info("Unexpected contactId at index: " + index);
                        return false;
                    }
                    if (index == expectedIds.size() - 1) {
                        // Sometimes two accounts are combined in one avro file
                        index = 0;
                    } else {
                        index++;
                    }
                }
            }
        }

        return true;
    }

    private static String getString(GenericRecord record, String field) throws Exception {
        String value;
        try {
            value = record.get(field).toString();
        } catch (Exception e) {
            value = "";
        }
        return value;
    }

    private static Map<String, List<String>> getExpectedIdsMap() {
        Map<String, List<String>> idMap = new HashMap<>();
        List<String> account1 = Arrays.asList("C11", "C12");
        List<String> account2 = Arrays.asList("C21", "C22");
        List<String> account3 = Arrays.asList("C3");
        List<String> account4 = Arrays.asList("C4");
        List<String> account5 = Arrays.asList("C5");
        List<String> account6 = Arrays.asList("C6");
        List<String> account7 = Arrays.asList("C71", "C72");
        List<String> account8 = Arrays.asList("C81", "C82");
        List<String> account9 = Arrays.asList("C91", "C92");
        List<String> account10 = Arrays.asList("C110", "C115");

        idMap.put("A1", account1);
        idMap.put("A2", account2);
        idMap.put("A3", account3);
        idMap.put("A4", account4);
        idMap.put("A5", account5);
        idMap.put("A6", account6);
        idMap.put("A7", account7);
        idMap.put("A8", account8);
        idMap.put("A9", account9);
        idMap.put("A10", account10);

        return idMap;
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactAccountRatioExceed() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        config.setWorkspace("testGenerateLaunchUniverseJobBothLimits");

        config.setContactAccountRatioThreshold(3L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setContactsPerAccountSortDirection(DESC);

        log.info("Config: " + JsonUtils.serialize(config));
        try {
            SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        } catch (RuntimeException e){
            return;
        }
        Assert.fail("Failed in testGenerateLaunchUniverseJobContactAccountRatioExceed\n");
    }

    @SuppressWarnings("unchecked")
    private static <T extends AvroExportable> void createAvroFromJson(String fileName, String jsonPath, Schema schema,
            Class<T> elementClazz, Configuration yarnConfiguration) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream tableRegistryStream = classLoader.getResourceAsStream(jsonPath);
        String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
        List<T> accounts = JsonUtils.convertList(raw, elementClazz);
        String avroPath = "/tmp/testGenerateLaunchUniverse" + fileName + AVRO_EXTENSION;

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
    }

    interface AvroExportable {
        GenericRecord getAsRecord(Schema schema);
    }

    static class Contact implements AvroExportable {
        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        @JsonProperty(value = "AccountId")
        private String accountId;

        public String getContactId() {
            return contactId;
        }

        public void setContactId(String contactId) {
            this.contactId = contactId;
        }

        @JsonProperty(value = "ContactId")
        private String contactId;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", this.getAccountId());
            builder.set("ContactId", this.getContactId());
            return builder.build();
        }
    }

    private void logHDFSDataUnit(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return;
        }
        log.info(tag + ", " + dataUnit.toString());
    }

}
