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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.CalculateDeltaJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculateDeltaJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CalculateDeltaJobTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    private DataUnit previousAccounts;
    private DataUnit currentAccounts;
    private DataUnit previousContacts;
    private DataUnit previousS3Contacts;
    private DataUnit currentContacts;

    @BeforeClass(groups = "functional")
    public void setup() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();

        Schema accountSchema = SchemaBuilder.record("Account").fields() //
                .name("AccountId").type().stringType().noDefault()//
                .endRecord();

        Schema contactSchema = SchemaBuilder.record("Contact").fields() //
                .name("ContactId").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("AccountId").type().stringType().noDefault() //
                .endRecord();
        String extension = ".avro";
        try {
            String fileName = "PreviousAccounts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    accountSchema, Account.class, yarnConfiguration);
            previousAccounts = HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName));

            fileName = "CurrentAccounts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    accountSchema, Account.class, yarnConfiguration);
            currentAccounts = HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName));

            fileName = "PreviousContacts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            previousContacts = HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName));

            fileName = "CurrentContacts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            currentContacts = HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName));

            fileName = "PreviousS3Contacts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            previousS3Contacts = HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testCalculateDelta" + fileName));
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        super.setup();

        // Spark repl setup
        // val newDFAlias = "newDfAlias"
        // val oldDFAlias = "oldDFAlias"
        //
        // val newData
        // =spark.read.format("avro").load("/tmp/testCalculateDeltaCurrentAccounts.avro")
        // val oldData =
        // spark.read.format("avro").load("/tmp/testCalculateDeltaPreviousAccounts.avro")
        //
        // val oldCData =
        // spark.read.format("avro").load("/tmp/testCalculateDeltaPreviousContacts.avro")
        // val newCData
        // =spark.read.format("avro").load("/tmp/testCalculateDeltaCurrentContacts.avro")
    }

    @Test(groups = "functional")
    public void testCalculateDeltaSalesForceUseCase() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(previousAccounts);
        config.setNewData(currentAccounts);
        config.setPrimaryJoinKey(InterfaceName.AccountId.name());
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 2);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 1);
    }

    @Test(groups = "functional")
    public void testCalculateDeltaMarketoUseCase() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(previousContacts);
        config.setNewData(currentContacts);
        config.setPrimaryJoinKey(InterfaceName.ContactId.name());
        config.setFilterPrimaryJoinKeyNulls(true);
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 2);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
    }

    @Test(groups = "functional")
    public void testCalculateDeltaS3UseCase() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(previousS3Contacts);
        config.setNewData(currentContacts);
        config.setPrimaryJoinKey(InterfaceName.ContactId.name());
        config.setSecondaryJoinKey(InterfaceName.AccountId.name());
        config.setFilterPrimaryJoinKeyNulls(false);
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 4);
    }

    @Test(groups = "functional")
    public void testCalculateFirstTimeAccountDelta() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(null);
        config.setNewData(currentAccounts);
        config.setPrimaryJoinKey(InterfaceName.AccountId.name());
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 7);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 0);
    }

    @Test(groups = "functional")
    public void testCalculateFirstTimeContactDelta() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(null);
        config.setNewData(currentContacts);
        config.setPrimaryJoinKey(InterfaceName.ContactId.name());
        config.setFilterPrimaryJoinKeyNulls(true);
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 7);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 0);
    }

    @Test(groups = "functional")
    public void testCalculateFirstTimeContactDeltaWithoutJoinKeyNulls() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(null);
        config.setNewData(currentContacts);
        config.setPrimaryJoinKey(InterfaceName.ContactId.name());
        config.setFilterPrimaryJoinKeyNulls(false);
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 9);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 0);
    }

    @Test(groups = "functional")
    public void testCalculateNoChange() {
        CalculateDeltaJobConfig config = new CalculateDeltaJobConfig();
        config.setOldData(previousContacts);
        config.setNewData(previousContacts);
        config.setPrimaryJoinKey(InterfaceName.ContactId.name());
        config.setFilterPrimaryJoinKeyNulls(false);
        log.info(JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(CalculateDeltaJob.class, config);
        Assert.assertEquals(result.getTargets().size(), 2);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 0);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 0);
    }

    interface AvroExportable {
        GenericRecord getAsRecord(Schema schema);
    }

    static class Account implements AvroExportable {
        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        @JsonProperty(value = "AccountId")
        private String accountId;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", this.getAccountId());
            return builder.build();
        }

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

    @SuppressWarnings("unchecked")
    private static <T extends AvroExportable> void createAvroFromJson(String fileName, String jsonPath, Schema schema,
            Class<T> elementClazz, Configuration yarnConfiguration) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream tableRegistryStream = classLoader.getResourceAsStream(jsonPath);
        String attributesDoc = StreamUtils.copyToString(tableRegistryStream, Charset.defaultCharset());
        List<Object> raw = JsonUtils.deserialize(attributesDoc, List.class);
        List<T> accounts = JsonUtils.convertList(raw, elementClazz);
        String extension = ".avro";
        String avroPath = "/tmp/testCalculateDelta" + fileName + extension;

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

    private void logHDFSDataUnit(String tag, HdfsDataUnit dataUnit) {
        if (dataUnit == null) {
            return;
        }
        String valueSeparator = ": ";
        String tokenSeparator = ", ";
        log.info(tag + tokenSeparator //
                + "StorageType: " + valueSeparator + dataUnit.getStorageType().name() + tokenSeparator //
                + "Path: " + valueSeparator + dataUnit.getPath() + tokenSeparator //
                + "Count: " + valueSeparator + dataUnit.getCount());
    }
}
