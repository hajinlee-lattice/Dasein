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
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.spark.cdl.GenerateLaunchArtifactsJobConfig;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class GenerateLaunchArtifactsJobTestNG extends SparkJobFunctionalTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenerateLaunchArtifactsJobTestNG.class);

    @Inject
    private Configuration yarnConfiguration;

    private DataUnit positiveAccounts;
    private DataUnit negativeAccounts;
    private DataUnit positiveContacts;
    private DataUnit negativeContacts;
    private DataUnit accountData;
    private DataUnit contactData;

    @BeforeClass(groups = "functional")
    public void setup() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();

        Schema deltaAccountSchema = SchemaBuilder.record("Account").fields() //
                .name("AccountId").type().stringType().noDefault()//
                .endRecord();

        Schema deltaContactSchema = SchemaBuilder.record("Contact").fields() //
                .name("ContactId").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("AccountId").type().stringType().noDefault() //
                .endRecord();

        Schema accountSchema = SchemaBuilder.record("Account").fields() //
                .name("AccountId").type().stringType().noDefault()//
                .name("engine_qkkfxxvgtbc9jkaymn80cq")
                .type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("CompanyName").type().stringType().noDefault()//
                .name("user_TimeZone_Test2").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion())
                .noDefault() //
                .name("AnnualRevenueCurrency").type().stringType().noDefault()//
                .name("Website").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("PostalCode").type().stringType().noDefault()//
                .name("engine_qkkfxxvgtbc9jkaymn80cq_score")
                .type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .endRecord();

        Schema contactSchema = SchemaBuilder.record("Contact").fields() //
                .name("ContactId").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("AccountId").type().stringType().noDefault() //
                .name("Email").type().stringType().noDefault() //
                .name("FirstName").type().stringType().noDefault() //
                .name("Title").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
                .name("user_Email_Opt_In").type().stringType().noDefault() //
                .name("LeadStatus").type().stringType().noDefault() //
                .endRecord();

        String extension = ".avro";
        try {
            String fileName = "positiveAccounts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaAccountSchema, DeltaAccount.class, yarnConfiguration);
            positiveAccounts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "negativeAccounts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaAccountSchema, DeltaAccount.class, yarnConfiguration);
            negativeAccounts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "positiveContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            positiveContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "negativeContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            negativeContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "account";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    accountSchema, Account.class, yarnConfiguration);
            accountData = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "contact";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            contactData = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        super.setup();

        // Spark repl setup
        // val accountId = "AccountId"
        //
        // val contactId = "ContactId"
        //
        // val accountAlias = "account"
        //
        // val contactAlias = "contact"
        //
        // val accountsDf =
        // spark.read.format("avro").load("/tmp/testGenerateLaunchArtifactsaccount.avro")
        //
        // val contactsDf =
        // spark.read.format("avro").load("/tmp/testGenerateLaunchArtifactscontact.avro")
        //
        // val positiveDeltaDf =
        // spark.read.format("avro").load("/tmp/testGenerateLaunchArtifactspositiveContacts.avro")
        //
        // val negativeDeltaDf =
        // spark.read.format("avro").load("/tmp/testGenerateLaunchArtifactsnegativeContacts.avro")
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForAccountEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        config.setAccountsData(accountData);
        config.setContactsData(contactData);
        config.setPositiveDelta(positiveAccounts);
        config.setNegativeDelta(negativeAccounts);
        config.setMainEntity(BusinessEntity.Account);
        config.setWorkspace("testGenerateLaunchArtifactsForAccountEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 3);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 4);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 5);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForContactEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        config.setAccountsData(accountData);
        config.setContactsData(contactData);
        config.setPositiveDelta(positiveContacts);
        config.setNegativeDelta(negativeContacts);
        config.setMainEntity(BusinessEntity.Contact);
        config.setWorkspace("testGenerateLaunchArtifactsForContactEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 5);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 7);
        Assert.assertEquals(result.getTargets().get(3).getCount().intValue(), 5);
        Assert.assertEquals(result.getTargets().get(4).getCount().intValue(), 4);
    }

    interface AvroExportable {
        GenericRecord getAsRecord(Schema schema);
    }

    static class DeltaAccount implements AvroExportable {
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

    static class Account implements AvroExportable {
        public String getAccountId() {
            return accountId;
        }

        public void setAccountId(String accountId) {
            this.accountId = accountId;
        }

        public String getEngine_qkkfxxvgtbc9jkaymn80cq() {
            return engine_qkkfxxvgtbc9jkaymn80cq;
        }

        public void setEngine_qkkfxxvgtbc9jkaymn80cq(String engine_qkkfxxvgtbc9jkaymn80cq) {
            this.engine_qkkfxxvgtbc9jkaymn80cq = engine_qkkfxxvgtbc9jkaymn80cq;
        }

        public String getCompanyName() {
            return companyName;
        }

        public void setCompanyName(String companyName) {
            this.companyName = companyName;
        }

        public String getUser_TimeZone_Test2() {
            return user_TimeZone_Test2;
        }

        public void setUser_TimeZone_Test2(String user_TimeZone_Test2) {
            this.user_TimeZone_Test2 = user_TimeZone_Test2;
        }

        public String getAnnualRevenueCurrency() {
            return AnnualRevenueCurrency;
        }

        public void setAnnualRevenueCurrency(String annualRevenueCurrency) {
            AnnualRevenueCurrency = annualRevenueCurrency;
        }

        public String getWebsite() {
            return Website;
        }

        public void setWebsite(String website) {
            Website = website;
        }

        public String getPostalCode() {
            return PostalCode;
        }

        public void setPostalCode(String postalCode) {
            PostalCode = postalCode;
        }

        public String getEngine_qkkfxxvgtbc9jkaymn80cq_score() {
            return engine_qkkfxxvgtbc9jkaymn80cq_score;
        }

        public void setEngine_qkkfxxvgtbc9jkaymn80cq_score(String engine_qkkfxxvgtbc9jkaymn80cq_score) {
            this.engine_qkkfxxvgtbc9jkaymn80cq_score = engine_qkkfxxvgtbc9jkaymn80cq_score;
        }

        @JsonProperty(value = "AccountId")
        private String accountId;

        @JsonProperty(value = "engine_qkkfxxvgtbc9jkaymn80cq")
        private String engine_qkkfxxvgtbc9jkaymn80cq;

        @JsonProperty(value = "CompanyName")
        private String companyName;

        @JsonProperty(value = "user_TimeZone_Test2")
        private String user_TimeZone_Test2;

        @JsonProperty(value = "AnnualRevenueCurrency")
        private String AnnualRevenueCurrency;

        @JsonProperty(value = "Website")
        private String Website;

        @JsonProperty(value = "PostalCode")
        private String PostalCode;

        @JsonProperty(value = "engine_qkkfxxvgtbc9jkaymn80cq_score")
        private String engine_qkkfxxvgtbc9jkaymn80cq_score;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", this.getAccountId());
            builder.set("engine_qkkfxxvgtbc9jkaymn80cq", this.getEngine_qkkfxxvgtbc9jkaymn80cq());
            builder.set("CompanyName", this.getCompanyName());
            builder.set("user_TimeZone_Test2", this.getUser_TimeZone_Test2());
            builder.set("AnnualRevenueCurrency", this.getAnnualRevenueCurrency());
            builder.set("Website", this.getWebsite());
            builder.set("PostalCode", this.getPostalCode());
            builder.set("engine_qkkfxxvgtbc9jkaymn80cq_score", this.getEngine_qkkfxxvgtbc9jkaymn80cq_score());
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

        public String getUser_Email_Opt_In() {
            return user_Email_Opt_In;
        }

        public void setUser_Email_Opt_In(String user_Email_Opt_In) {
            this.user_Email_Opt_In = user_Email_Opt_In;
        }

        public String getLeadStatus() {
            return LeadStatus;
        }

        public void setLeadStatus(String leadStatus) {
            LeadStatus = leadStatus;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        @JsonProperty(value = "AccountId")
        private String accountId;

        @JsonProperty(value = "ContactId")
        private String ContactId;

        @JsonProperty(value = "Email")
        private String Email;

        @JsonProperty(value = "FirstName")
        private String FirstName;

        @JsonProperty(value = "user_Email_Opt_In")
        private String user_Email_Opt_In;

        @JsonProperty(value = "LeadStatus")
        private String LeadStatus;

        @JsonProperty(value = "Title")
        private String title;

        @Override
        public GenericRecord getAsRecord(Schema schema) {
            GenericRecordBuilder builder = new GenericRecordBuilder(schema);
            builder.set("AccountId", this.getAccountId());
            builder.set("ContactId", this.getContactId());
            builder.set("Email", this.getEmail());
            builder.set("FirstName", this.getFirstName());
            builder.set("user_Email_Opt_In", this.getUser_Email_Opt_In());
            builder.set("LeadStatus", this.getLeadStatus());
            builder.set("Title", this.getTitle());
            return builder.build();
        }

    }

    static class DeltaContact implements AvroExportable {
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
        String avroPath = "/tmp/testGenerateLaunchArtifacts" + fileName + extension;

        File localFile = new File(avroPath);
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(new GenericDatumWriter<>(schema))) {
            dataFileWriter.create(schema, localFile);
            accounts.forEach(account -> {
                try {
                    dataFileWriter.append(account.getAsRecord(schema));
                } catch (IOException ioe) {
                    // Do Nothing
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
}
