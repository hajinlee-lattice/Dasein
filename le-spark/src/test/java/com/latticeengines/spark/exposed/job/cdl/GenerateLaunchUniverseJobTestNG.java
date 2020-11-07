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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchUniverseJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateLaunchUniverseJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchUniverseJobTestNG.class);

    private static final String CDL_UPDATED_TIME = InterfaceName.CDLUpdatedTime.name();

    @Inject
    private Configuration yarnConfiguration;

    private DataUnit contactData;

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();

        Schema contactSchema = SchemaBuilder.record("ContactLimitTest").fields() //
                .name("ContactId").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("AccountId").type().stringType().noDefault() //
                .name("Email").type().stringType().noDefault() //
                .name("FirstName").type().stringType().noDefault() //
                .name("Title").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name(CDL_UPDATED_TIME).type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .endRecord();

        String extension = ".avro";
        try {

            String fileName = "contactLimitTest";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            contactData = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchUniverse" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchUniverse" + fileName));

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        super.setup();

    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobContactLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        log.info("contactData: " + contactData);
        config.setLaunchData(contactData);
        config.setMaxContactsPerAccount(2L);
        config.setMaxAccountsToLaunch(20L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setWorkspace("testGenerateLaunchUniverseJobContactLimit");

        String encryptionKey = CipherUtils.generateKey();
        String saltHint = CipherUtils.generateKey();
        config.setManageDbUrl(url);
        config.setUser(user);
        config.setEncryptionKey(encryptionKey);
        config.setSaltHint(saltHint);
        config.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobContactLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 16);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobNoLimit() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        log.info("contactData: " + contactData);
        config.setLaunchData(contactData);
        config.setWorkspace("testGenerateLaunchUniverseJobNoLimit");

        String encryptionKey = CipherUtils.generateKey();
        String saltHint = CipherUtils.generateKey();
        config.setManageDbUrl(url);
        config.setUser(user);
        config.setEncryptionKey(encryptionKey);
        config.setSaltHint(saltHint);
        config.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobNoLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 20);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchUniverseJobBothLimits() throws Exception {
        GenerateLaunchUniverseJobConfig config = new GenerateLaunchUniverseJobConfig();
        log.info("contactData: " + contactData);
        config.setLaunchData(contactData);
        config.setMaxContactsPerAccount(2L);
        config.setMaxAccountsToLaunch(13L);
        config.setContactsPerAccountSortAttribute(CDL_UPDATED_TIME);
        config.setWorkspace("testGenerateLaunchUniverseJobBothLimits");

        String encryptionKey = CipherUtils.generateKey();
        String saltHint = CipherUtils.generateKey();
        config.setManageDbUrl(url);
        config.setUser(user);
        config.setEncryptionKey(encryptionKey);
        config.setSaltHint(saltHint);
        config.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchUniverseJob.class, config);
        log.info("TestGenerateLaunchUniverseJobBothLimits Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 13);

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

        public String getContactId() {
            return ContactId;
        }

        public void setContactId(String contactId) {
            ContactId = contactId;
        }

        public String getEmail() {
            return Email;
        }

        public void setEmail(String email) {
            Email = email;
        }

        public String getFirstName() {
            return FirstName;
        }

        public void setFirstName(String firstName) {
            FirstName = firstName;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getCDLUpdatedTime() {
            return CDLUpdatedTime;
        }

        public void setCDLUpdatedTime(String CDLUpdatedTime) {
            this.CDLUpdatedTime = CDLUpdatedTime;
        }

        @JsonProperty(value = "AccountId")
        private String accountId;

        @JsonProperty(value = "ContactId")
        private String ContactId;

        @JsonProperty(value = "Email")
        private String Email;

        @JsonProperty(value = "FirstName")
        private String FirstName;

        @JsonProperty(value = "Title")
        private String title;

        @JsonProperty(value = "CDLUpdatedTime")
        private String CDLUpdatedTime;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", this.getAccountId());
            builder.set("ContactId", this.getContactId());
            builder.set("Email", this.getEmail());
            builder.set("FirstName", this.getFirstName());
            builder.set("Title", this.getTitle());
            builder.set("CDLUpdatedTime", this.getTitle());
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
        String avroPath = "/tmp/testGenerateLaunchUniverse" + fileName + extension;

        File localFile = new File(avroPath);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, localFile);
            accounts.forEach(account -> {
                try {
                    dataFileWriter.append(account.getAsRecord(schema));
                } catch (IOException ioe) {
                    log.warn("failed to write a avro datum", ioe);
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
        log.info(tag + ", " + dataUnit.toString());
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

}
