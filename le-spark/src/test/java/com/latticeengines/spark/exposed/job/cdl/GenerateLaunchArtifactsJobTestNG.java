package com.latticeengines.spark.exposed.job.cdl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.util.StreamUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.HdfsUtils.HdfsFileFilter;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
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
    private DataUnit negativeExtraAccounts;
    private DataUnit positiveContacts;
    private DataUnit nullPositiveContacts;
    private DataUnit negativeContacts;
    private DataUnit negativeExtraContacts;
    private DataUnit accountData;
    private DataUnit contactData;
    private DataUnit contactNoContactCountryData;
    private DataUnit limitedContactsData;

    private HdfsFileFilter avroFileFilter = new HdfsFileFilter() {
        @Override
        public boolean accept(FileStatus file) {
            return file.getPath().getName().endsWith("avro");
        }
    };

    @Value("${datacloud.manage.url}")
    private String url;

    @Value("${datacloud.manage.user}")
    private String user;

    @Value("${datacloud.manage.password.encrypted}")
    private String password;

    @Override
    @BeforeClass(groups = "functional")
    public void setup() {
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
                .name("Country").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
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
                .name("ContactCountry").type(SchemaBuilder.unionOf().nullType().and().stringType().endUnion()).noDefault() //
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

            fileName = "negativeExtraAccounts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaAccountSchema, DeltaAccount.class, yarnConfiguration);
            negativeExtraAccounts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "positiveContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            positiveContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "nullPositiveContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            nullPositiveContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "negativeContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            negativeContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "negativeExtraContacts";
            createAvroFromJson(fileName, String
                    .format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sDelta.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            negativeExtraContacts = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
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

            fileName = "contactNoContactCountry";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    contactSchema, Contact.class, yarnConfiguration);
            contactNoContactCountryData = HdfsDataUnit
                    .fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

            fileName = "limitedContacts";
            createAvroFromJson(fileName,
                    String.format("com/latticeengines/common/exposed/util/SparkCountRecordsTest/%sData.json", fileName),
                    deltaContactSchema, DeltaContact.class, yarnConfiguration);
            limitedContactsData = HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName + extension);
            logHDFSDataUnit(fileName, HdfsDataUnit.fromPath("/tmp/testGenerateLaunchArtifacts" + fileName));

        } catch (Exception e) {
            log.error(e.getMessage());
        }
        super.setup();
    }

    private void putDataUnits(DataUnit accountDataUnit, DataUnit contactDataUnit, DataUnit targetSegmentsContactsDataUnit,
                              DataUnit negativeDeltaDataUnit, DataUnit positiveDeltaDataUnit, DataUnit perAccountLimitedContacts) {
        Map<String, DataUnit> inputUnits = new HashMap<>();
        inputUnits.put("Input0", accountDataUnit);
        inputUnits.put("Input1", contactDataUnit);
        inputUnits.put("Input2", targetSegmentsContactsDataUnit);
        inputUnits.put("Input3", negativeDeltaDataUnit);
        inputUnits.put("Input4", positiveDeltaDataUnit);
        inputUnits.put("Input5", perAccountLimitedContacts);
        setInputUnits(inputUnits);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForAccountEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, negativeAccounts, positiveAccounts, null);
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
    public void testGenerateLaunchArtifactsForAccountEntityWithoutContacts() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, null, null, negativeAccounts, positiveAccounts, null);
        config.setMainEntity(BusinessEntity.Account);
        config.setWorkspace("testGenerateLaunchArtifactsForAccountEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 3);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 4);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 0);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForExtraNegativeAccountEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, negativeExtraAccounts, positiveAccounts, null);
        config.setMainEntity(BusinessEntity.Account);
        config.setWorkspace("testGenerateLaunchArtifactsForContactEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsForExtraNegativeAccountEntity Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 3);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 4);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 4);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 5);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForContactEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, negativeContacts, positiveContacts, null);
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

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForExtraNegativeContactEntity() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, negativeExtraContacts, positiveContacts, null);
        config.setMainEntity(BusinessEntity.Contact);
        config.setWorkspace("testGenerateLaunchArtifactsForContactEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsForExtraNegativeContactEntity Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 5);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 3);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 7);
        Assert.assertEquals(result.getTargets().get(3).getCount().intValue(), 5);
        Assert.assertEquals(result.getTargets().get(4).getCount().intValue(), 5);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsForNullPositiveContacts() {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, null, nullPositiveContacts, null);
        config.setMainEntity(BusinessEntity.Contact);
        config.setIncludeAccountsWithoutContacts(true);
        config.setWorkspace("testGenerateLaunchArtifactsForContactEntity");

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsForExtraNegativeContactEntity Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().size(), 5);
        Assert.assertEquals(result.getTargets().get(0).getCount().intValue(), 5);
        Assert.assertEquals(result.getTargets().get(1).getCount().intValue(), 0);
        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 7);
        Assert.assertEquals(result.getTargets().get(3).getCount().intValue(), 6);
        Assert.assertEquals(result.getTargets().get(4).getCount().intValue(), 0);
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsJobForContactCountyConversion() throws Exception {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, null, positiveContacts, null);
        config.setMainEntity(BusinessEntity.Contact);
        config.setWorkspace("testGenerateLaunchArtifactsJobForContactCountyConversion");
        config.setExternalSystemName(CDLExternalSystemName.GoogleAds);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsJobForContactCountyConversion Results: " + JsonUtils.serialize(result));
        // result.getTargets should have [addedAccountsData,
        // removedAccountsData, fullContactsData, addedContactsData,
        // removedContactsData]
        // only fullContactsData and addedContactsData shuould be converted
        testCountryConversion(yarnConfiguration, result.getTargets().get(2).getPath(), avroFileFilter,
                InterfaceName.ContactCountry.name());
        testCountryConversion(yarnConfiguration, result.getTargets().get(3).getPath(), avroFileFilter,
                InterfaceName.ContactCountry.name());
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsJobForAccountCountyConversion() throws Exception {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, null, null, positiveAccounts, null);
        config.setMainEntity(BusinessEntity.Account);
        config.setWorkspace("testGenerateLaunchArtifactsJobForAccountCountyConversion");
        config.setExternalSystemName(CDLExternalSystemName.LinkedIn);

        String encryptionKey = CipherUtils.generateKey();
        String saltHint = CipherUtils.generateKey();
        config.setManageDbUrl(url);
        config.setUser(user);
        config.setEncryptionKey(encryptionKey);
        config.setSaltHint(saltHint);
        config.setPassword(CipherUtils.encrypt(password, encryptionKey, saltHint));

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsJobForAccountCountyConversion Results: " + JsonUtils.serialize(result));
        // result.getTargets should have [addedAccountsData,
        // removedAccountsData, fullContactsData]
        // only addedAccountsData should be converted
        testCountryConversion(yarnConfiguration, result.getTargets().get(0).getPath(), avroFileFilter,
                InterfaceName.Country.name());
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsJobWithNoCountry() throws Exception {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactNoContactCountryData, contactNoContactCountryData, null, positiveContacts, null);
        config.setMainEntity(BusinessEntity.Contact);
        config.setWorkspace("testGenerateLaunchArtifactsJobWithNoCountry");
        config.setExternalSystemName(CDLExternalSystemName.GoogleAds);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("TestGenerateLaunchArtifactsJobForContactCountyConversion Results: " + JsonUtils.serialize(result));
        // result.getTargets should have [addedAccountsData,
        // removedAccountsData, fullContactsData, addedContactsData,
        // removedContactsData]
        // only fullContactsData and addedContactsData shuould be converted
        testCountryConversion(yarnConfiguration, result.getTargets().get(2).getPath(), avroFileFilter,
                InterfaceName.ContactCountry.name());
        testCountryConversion(yarnConfiguration, result.getTargets().get(3).getPath(), avroFileFilter,
                InterfaceName.ContactCountry.name());
    }

    @Test(groups = "functional")
    public void testGenerateLaunchArtifactsJobContactLimit() throws Exception {
        GenerateLaunchArtifactsJobConfig config = new GenerateLaunchArtifactsJobConfig();
        putDataUnits(accountData, contactData, contactData, null, positiveContacts, limitedContactsData);
        config.setExternalSystemName(CDLExternalSystemName.Eloqua);
        config.setMainEntity(BusinessEntity.Contact);
        config.setWorkspace("testGenerateLaunchArtifactsJobContactLimit");
        config.setUseContactsPerAccountLimit(true);

        log.info("Config: " + JsonUtils.serialize(config));
        SparkJobResult result = runSparkJob(GenerateLaunchArtifactsJob.class, config);
        log.info("testGenerateLaunchArtifactsJobContactLimit Results: " + JsonUtils.serialize(result));

        Assert.assertEquals(result.getTargets().get(2).getCount().intValue(), 3);
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

        public String getCountry() {
            return Country;
        }

        public void setCountry(String country) {
            Country = country;
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

        @JsonProperty(value = "Country")
        private String Country;

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
            builder.set("Country", this.getCountry());
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

        public String getContactCountry() {
            return ContactCountry;
        }

        public void setContactCountry(String contactCountry) {
            ContactCountry = contactCountry;
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

        @JsonProperty(value = "ContactCountry")
        private String ContactCountry;

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
            builder.set("ContactCountry", this.getContactCountry());
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

    private void testCountryConversion(Configuration yarnConfiguration, String hdfsDir, HdfsFileFilter avroFileFilter, String fieldName) throws Exception {
        List avroFilePaths = HdfsUtils.onlyGetFilesForDir(yarnConfiguration, hdfsDir, avroFileFilter);
        for (Object filePath : avroFilePaths) {
            String filePathStr = filePath.toString();
            log.info("File path is: " + filePathStr);

            try (FileReader<GenericRecord> reader = AvroUtils.getAvroFileReader(yarnConfiguration, new Path(filePathStr))) {
                for (GenericRecord record : reader) {
                    String country = getString(record, fieldName);
                    if (!country.isEmpty()) {
                        log.info(fieldName + ": " + country);
                        Assert.assertTrue(country.length() == 2);
                    }
                }
            }
        }
    }
}
