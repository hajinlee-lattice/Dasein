package com.latticeengines.spark.exposed.job.cdl;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.contactmaster.ContactMasterConstants;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLiveRampLaunchArtifactsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateLiveRampLaunchArtifactsJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenerateLiveRampLaunchArtifactsJobTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    private static String HDFS_DIR = "/tmp/testGenerateLiveRampLaunchArtifacts/";
    private DataUnit noDuplicateRecordId;
    private DataUnit oneDuplicateRecordId;
    private DataUnit manyDuplicateRecordId;
    private DataUnit manyDiffDuplicateRecordId;

    private static Long NO_DUPLICATED_EXPECTED = 500L;
    private static Long ONE_DUPLICATE_EXPECTED = 499L;
    private static Long MANY_DUPLICATE_EXPECTED = 464L;
    private static Long MANY_DIFF_DUPLICATE_EXPECTED = 50L;
    private static Long ZERO_EXPECTED = 0L;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        Schema contactSchema = SchemaBuilder.record("Contact").fields() //
                .name(ContactMasterConstants.TPS_ATTR_RECORD_ID)
                .type(SchemaBuilder.unionOf().nullType().and().stringType()
                        .endUnion())
                .noDefault() //
                .endRecord();

        String extension = ".avro";
        try {
            if (!HdfsUtils.isDirectory(yarnConfiguration, HDFS_DIR)) {
                log.info("Dir does not exist, creating dir: " + HDFS_DIR);
                HdfsUtils.mkdir(yarnConfiguration, HDFS_DIR);
            }

            String fileName = "noDuplicateRecordId";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/GenerateLiveRampLaunchArtifacts/%s.json",
                            fileName),
                    contactSchema, LiveRampContact.class, yarnConfiguration);
            noDuplicateRecordId = HdfsDataUnit
                    .fromPath(HDFS_DIR + fileName + extension);
            logHDFSDataUnit(fileName, (HdfsDataUnit) noDuplicateRecordId);

            fileName = "oneDuplicateRecordId";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/GenerateLiveRampLaunchArtifacts/%s.json",
                            fileName),
                    contactSchema, LiveRampContact.class, yarnConfiguration);
            oneDuplicateRecordId = HdfsDataUnit
                    .fromPath(HDFS_DIR + fileName + extension);
            logHDFSDataUnit(fileName, (HdfsDataUnit) oneDuplicateRecordId);

            fileName = "manyDuplicateRecordId";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/GenerateLiveRampLaunchArtifacts/%s.json",
                            fileName),
                    contactSchema, LiveRampContact.class, yarnConfiguration);
            manyDuplicateRecordId = HdfsDataUnit
                    .fromPath(HDFS_DIR + fileName + extension);
            logHDFSDataUnit(fileName, (HdfsDataUnit) manyDuplicateRecordId);

            fileName = "manyDiffDuplicateRecordId";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/GenerateLiveRampLaunchArtifacts/%s.json",
                            fileName),
                    contactSchema, LiveRampContact.class, yarnConfiguration);
            manyDiffDuplicateRecordId = HdfsDataUnit
                    .fromPath(HDFS_DIR + fileName + extension);
            logHDFSDataUnit(fileName, (HdfsDataUnit) manyDiffDuplicateRecordId);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        super.setup();
    }

    @AfterClass(groups = "functional")
    public void cleanupHdfsFiles() {
        try {
            HdfsUtils.rmdir(yarnConfiguration, HDFS_DIR);
        } catch (IOException e) {
            log.info("Failed to clean up: {}", HDFS_DIR);
        }
    }

    @Test(groups = "functional")
    public void testNoDuplicateRecordIds() {
        Long expectedAddContacts = NO_DUPLICATED_EXPECTED;
        Long expectedRemoveContacts = NO_DUPLICATED_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setAddContacts(noDuplicateRecordId);
        config.setRemoveContacts(noDuplicateRecordId);
        config.setWorkspace("testNoDuplicateRecordIds");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), expectedAddContacts);
        Assert.assertEquals(result.getTargets().get(1).getCount(), expectedRemoveContacts);
    }

    @Test(groups = "functional")
    public void testOneDuplicateRecordIds() {
        Long expectedAddContacts = ONE_DUPLICATE_EXPECTED;
        Long expectedRemoveContacts = ONE_DUPLICATE_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setAddContacts(oneDuplicateRecordId);
        config.setRemoveContacts(oneDuplicateRecordId);
        config.setWorkspace("testOneDuplicateRecordIds");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), expectedAddContacts);
        Assert.assertEquals(result.getTargets().get(1).getCount(), expectedRemoveContacts);
    }

    @Test(groups = "functional")
    public void testManyDuplicateRecordIds() {
        Long expectedAddContacts = MANY_DUPLICATE_EXPECTED;
        Long expectedRemoveContacts = MANY_DUPLICATE_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setAddContacts(manyDuplicateRecordId);
        config.setRemoveContacts(manyDuplicateRecordId);
        config.setWorkspace("testManyDuplicateRecordIds");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), expectedAddContacts);
        Assert.assertEquals(result.getTargets().get(1).getCount(), expectedRemoveContacts);
    }

    @Test(groups = "functional")
    public void testManyDiffDuplicateRecordIds() {
        Long expectedAddContacts = MANY_DIFF_DUPLICATE_EXPECTED;
        Long expectedRemoveContacts = MANY_DIFF_DUPLICATE_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setAddContacts(manyDiffDuplicateRecordId);
        config.setRemoveContacts(manyDiffDuplicateRecordId);
        config.setWorkspace("testManyDiffDuplicateRecordIds");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), expectedAddContacts);
        Assert.assertEquals(result.getTargets().get(1).getCount(), expectedRemoveContacts);
    }

    @Test(groups = "functional")
    public void testOnlyAddContacts() {
        Long expectedAddContacts = MANY_DIFF_DUPLICATE_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setAddContacts(manyDiffDuplicateRecordId);
        config.setWorkspace("testOnlyAddContacts");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), expectedAddContacts);
        Assert.assertEquals(result.getTargets().get(1).getCount(), ZERO_EXPECTED);
    }

    @Test(groups = "functional")
    public void testOnlyRemoveContacts() {
        Long expectedRemoveContacts = MANY_DIFF_DUPLICATE_EXPECTED;

        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();
        config.setRemoveContacts(manyDiffDuplicateRecordId);
        config.setWorkspace("testOnlyRemoveContacts");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), ZERO_EXPECTED);
        Assert.assertEquals(result.getTargets().get(1).getCount(), expectedRemoveContacts);
    }

    @Test(groups = "functional")
    public void testNoAddOrRemoveContacts() {
        GenerateLiveRampLaunchArtifactsJobConfig config = new GenerateLiveRampLaunchArtifactsJobConfig();

        config.setWorkspace("testNoAddOrRemoveContacts");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLiveRampLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount(), ZERO_EXPECTED);
        Assert.assertEquals(result.getTargets().get(1).getCount(), ZERO_EXPECTED);
    }

    interface AvroExportable {
        GenericRecord getAsRecord(Schema schema);
    }

    static class LiveRampContact implements AvroExportable {
        @JsonProperty(value = "RecordId")
        private String recordId;

        public String getRecordId() {
            return recordId;
        }

        public void setRecordId(String recordId) {
            this.recordId = recordId;
        }

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set(ContactMasterConstants.TPS_ATTR_RECORD_ID, this.getRecordId());
            return builder.build();
        }
    }

    private void logHDFSDataUnit(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return;
        }
        log.info(tag + ", " + JsonUtils.serialize(dataUnit));
    }

    @SuppressWarnings("unchecked")
    private static <T extends AvroExportable> void createAvroFromJson(String fileName, String jsonPath, Schema schema,
            Class<T> elementClazz, Configuration yarnConfiguration) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream tableRegistryStream = classLoader.getResourceAsStream(jsonPath);
        String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
        List<T> contacts = JsonUtils.convertList(raw, elementClazz);
        String extension = ".avro";
        String avroPath = HDFS_DIR + fileName + extension;

        String localPath = "/tmp/testGenerateLiveRampLaunchArtifacts" + fileName + extension;
        File localFile = new File(localPath);

        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, localFile);
            contacts.forEach(contact -> {
                try {
                    dataFileWriter.append(contact.getAsRecord(schema));
                } catch (IOException ioe) {
                    log.warn("failed to write a avro datdum", ioe);
                }
            });
            dataFileWriter.flush();
        }
        HdfsUtils.copyLocalToHdfs(yarnConfiguration, localFile.getAbsolutePath(), avroPath);
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, avroPath));
    }
}
