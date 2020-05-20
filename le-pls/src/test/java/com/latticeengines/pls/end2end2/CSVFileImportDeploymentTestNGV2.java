package com.latticeengines.pls.end2end2;

import static com.latticeengines.common.exposed.util.TimeStampConvertUtils.computeTimestamp;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.closeable.resource.CloseableResourcePool;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinition;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionSectionName;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CSVFileImportDeploymentTestNGV2 extends CSVFileImportDeploymentTestNGBaseV2 {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportDeploymentTestNGV2.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private ActionProxy actionProxy;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;


    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
    }

    @Test(groups = "deployment")
    public void testExternalSystem() throws Exception {
        String systemName = "ExternalSystem";
        SourceFile accountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(), ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), systemName);
        if(s3ImportSystem == null) {
            cdlService.createS3ImportSystem(mainTestTenant.getId(), systemName, S3ImportSystem.SystemType.Other,
                    false);
        }
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(systemName, DEFAULT_SYSTEM_TYPE,
                        EntityType.Accounts.getDisplayName(), accountFile.getName());
        FieldDefinitionsRecord fieldDefinitionsRecord =
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();

        FieldDefinition crmID = new FieldDefinition();
        crmID.setColumnName("ID");
        crmID.setFieldName("SFDC ID");
        crmID.setFieldType(UserDefinedType.TEXT);
        crmID.setExternalSystemType(CDLExternalSystemType.CRM);

        FieldDefinition mapID = new FieldDefinition();
        mapID.setColumnName("ID");
        mapID.setFieldName("MAP_System");
        mapID.setFieldType(UserDefinedType.TEXT);
        mapID.setExternalSystemType(CDLExternalSystemType.MAP);

        List<FieldDefinition> otherIds =
                fieldDefinitionsRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Other_IDs.getName());
        otherIds.add(crmID);
        otherIds.add(mapID);

        dataMappingService.commitFieldDefinitions(systemName, DEFAULT_SYSTEM_TYPE,
                EntityType.Accounts.getDisplayName(), accountFile.getName(), false, fieldDefinitionsRecord);

        String feedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.Accounts);
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, accountFile.getName(),
                accountFile.getName(), SOURCE, EntityType.Accounts.getEntity().name(), feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<Action> actions = actionProxy.getActions(customerSpace);
        validateImportAction(actions);
        validateJobsPage();

        DataFeedTask externalAccount = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType,
                EntityType.Accounts.getEntity().name());
        Assert.assertNotNull(externalAccount.getImportTemplate().getAttribute("user_SFDC_ID"));
        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace,
                EntityType.Accounts.getEntity().name());
        Assert.assertNotNull(system);
        Assert.assertTrue(system.getCRMIdList().contains("user_SFDC_ID"));
        Assert.assertEquals(system.getDisplayNameById("user_SFDC_ID"), "SFDC ID");
    }


    // the test case failed because the field name for ID is CustomerAccountId while not AccountId
    @Test(groups = "deployment", enabled = false)
    public void testFormatDate() throws Exception {
        SourceFile accountFile = fileUploadService.uploadFile(
                "file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(),
                ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader
                        .getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));
        String systemName = "FormatDate";
        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), systemName);
        if(s3ImportSystem == null) {
            cdlService.createS3ImportSystem(mainTestTenant.getId(), systemName,
                    S3ImportSystem.SystemType.Other, false);
        }
        FetchFieldDefinitionsResponse fieldDefinitionsResponse  =
                dataMappingService.fetchFieldDefinitions(systemName, DEFAULT_SYSTEM_TYPE,
                        EntityType.Accounts.getDisplayName(), accountFile.getName());

        String dateFormatString1 = "DD/MM/YYYY";
        String timeFormatString1 = null;
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        String dateFormatString3 = "YYYY-MMM-DD";
        String timeFormatString3 = "00:00 12H";
        String timezone3 = "America/Chicago";
        String dateFormatString4 = "YYYY-MM-DD";
        String timeFormatString4 = "00:00:00 24H";
        String timezone4 = TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE;
        FieldDefinitionsRecord currentRecord = fieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                currentRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Custom_Fields.getName())) {
            if (definition.getColumnName().equals("TestDate1")) {
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(dateFormatString1);
                definition.setTimeFormat(timeFormatString1);
                definition.setTimeZone(timezone1);
            } else if (definition.getColumnName().equals("TestDate2")) {
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(dateFormatString2);
                definition.setTimeFormat(timeFormatString2);
                definition.setTimeZone(timezone2);
            } else if (definition.getColumnName().equals("TestDate3")) {
                definition.setFieldType(UserDefinedType.DATE);
                definition.setDateFormat(dateFormatString3);
                definition.setTimeFormat(timeFormatString3);
                definition.setTimeZone(timezone3);
            } else if (definition.getColumnName().equals("TestTZDate")) {
                // this verify the TZ date can be validated through import workflow
                Assert.assertEquals(UserDefinedType.DATE, definition.getFieldType());
                Assert.assertEquals(dateFormatString4, definition.getDateFormat());
                Assert.assertEquals(timeFormatString4, definition.getTimeFormat());
                Assert.assertEquals(timezone4, definition.getTimeZone());
            }
        }

        dataMappingService.commitFieldDefinitions(systemName,
                DEFAULT_SYSTEM_TYPE, EntityType.Accounts.getDisplayName(), accountFile.getName(),
                false, currentRecord);

        String feedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.Accounts);
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace,
                accountFile.getName(), accountFile.getName(), SOURCE, EntityType.Accounts.getEntity().name(), feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(),
                false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        String avroFileName = accountFile.getName().substring(0,
                accountFile.getName().lastIndexOf("."));
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath, file ->
                !file.isDirectory() && file.getPath().toString().contains(avroFileName)
                        && file.getPath().getName().endsWith("avro"));
        Assert.assertEquals(avroFiles.size(), 1);
        String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));
        long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");
        log.info("File path to avros is: " + avroFilePath + "/*.avro");
        // modify file to match T&Z in 30th data line
        Assert.assertEquals(rowCount, 49);

        // Validate TestDate1
        String fieldName1 = "user_TestDate1";
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFiles.get(0)));
        Assert.assertEquals(schema.getField(fieldName1).schema().getTypes().get(0).getType(), Schema.Type.LONG);
        Assert.assertEquals(schema.getField(fieldName1).getProp("DateFormatString"), dateFormatString1);
        Assert.assertEquals(schema.getField(fieldName1).getProp("TimeFormatString"), timeFormatString1);
        Assert.assertEquals(schema.getField(fieldName1).getProp("Timezone"), timezone1);
        // Check first three values of column.
        List<GenericRecord> records = AvroUtils.getData(yarnConfiguration, new Path(avroFiles.get(0)));
        long expected1a = computeTimestamp("27/7/2017", false, "d/M/yyyy",
                "UTC-4");
        Assert.assertEquals(expected1a, 1501128000000L);
        Assert.assertEquals(records.get(0).get(fieldName1).toString(), Long.toString(expected1a));
        long expected1b = computeTimestamp("27/7/2018", false, "d/M/yyyy",
                "UTC-4");
        Assert.assertEquals(expected1b, 1532664000000L);
        Assert.assertEquals(records.get(1).get(fieldName1).toString(), Long.toString(expected1b));
        long expected1c = computeTimestamp("27/7/2019", false, "d/M/yyyy",
                "UTC-4");
        Assert.assertEquals(expected1c, 1564200000000L);
        Assert.assertEquals(records.get(2).get(fieldName1).toString(), Long.toString(expected1c));
        // The 4th row has a value that is pre-Epoch so 0 should be returned as the timestamp.
        long expected1d = 0L;
        Assert.assertEquals(records.get(3).get(fieldName1).toString(), Long.toString(expected1d));
        log.info("Value returned for input 31/12/1969 America/New_York was: "
                + records.get(3).get(fieldName1).toString());


        // Validate TestDate2
        String fieldName2 = "user_TestDate2";
        Assert.assertEquals(schema.getField(fieldName2).schema().getTypes().get(0).getType(), Schema.Type.LONG);
        Assert.assertEquals(schema.getField(fieldName2).getProp("DateFormatString"), dateFormatString2);
        Assert.assertEquals(schema.getField(fieldName2).getProp("TimeFormatString"), timeFormatString2);
        Assert.assertEquals(schema.getField(fieldName2).getProp("Timezone"), timezone2);
        // Check first three values of column.
        long expected2a = computeTimestamp("1.2.18 1:1:1", true,
                "M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2a, 1514826061000L);
        Assert.assertEquals(records.get(0).get(fieldName2).toString(), Long.toString(expected2a));
        long expected2b = computeTimestamp("12.31.18 23:59:59", true,"M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2b, 1546271999000L);
        Assert.assertEquals(records.get(1).get(fieldName2).toString(), Long.toString(expected2b));
        long expected2c = computeTimestamp("5.6.99 12:34:56", true,
                "M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2c, 4081725296000L);
        Assert.assertEquals(records.get(2).get(fieldName2).toString(), Long.toString(expected2c));
        // Test that two digit date which if it had started with "19" instead of "20" would be pre-Epoch time, works
        // correctly.
        long expected2d = computeTimestamp("12.12.68 12:12:12", true,
                "M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2d, 3122511132000L);
        Assert.assertEquals(records.get(4).get(fieldName2).toString(), Long.toString(expected2d));
        log.info("Value returned for input 12.12.68 12:12:12 Asia/Shanghai was: "
                + records.get(4).get(fieldName2).toString());

        // Validate TestDate3
        String fieldName3 = "user_TestDate3";
        Assert.assertEquals(schema.getField(fieldName3).schema().getTypes().get(0).getType(), Schema.Type.LONG);
        Assert.assertEquals(schema.getField(fieldName3).getProp("DateFormatString"), dateFormatString3);
        Assert.assertEquals(schema.getField(fieldName3).getProp("TimeFormatString"), timeFormatString3);
        Assert.assertEquals(schema.getField(fieldName3).getProp("Timezone"), timezone3);
        // Check first three values of column.
        long expected3a = computeTimestamp("1969-Dec-31 6:10 PM", true,
                "yyyy-MMM-d h:m a", "GMT-6");
        Assert.assertEquals(expected3a, 600000L);
        Assert.assertEquals(records.get(0).get(fieldName3).toString(), Long.toString(expected3a));
        long expected3b = computeTimestamp("2069-Dec-31 7:15 PM", true,
                "yyyy-MMM-d h:m a", "GMT-6");
        Assert.assertEquals(expected3b, 3155764500000L);
        Assert.assertEquals(records.get(1).get(fieldName3).toString(), Long.toString(expected3b));
        long expected3c = computeTimestamp("2019-Nov-11 11:11 AM", true,
                "yyyy-MMM-d h:m a", "GMT-6");
        Assert.assertEquals(expected3c, 1573492260000L);
        Assert.assertEquals(records.get(2).get(fieldName3).toString(), Long.toString(expected3c));
        long expected3d = computeTimestamp("1980-Jul-18 8:00 AM", true,
                "yyyy-MMM-d h:m a", "GMT-5");
        Assert.assertEquals(expected3d, 332773200000L);
        Assert.assertEquals(records.get(3).get(fieldName3).toString(), Long.toString(expected3d));
    }

    @Test(groups = "deployment")
    public void testExternalSystemWithContact() throws Exception {
        SourceFile accountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Contacts.getSchemaInterpretation(), EntityType.Contacts.getEntity().name(), CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String systemName = "ExternalSystem";
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.Contacts);
        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), systemName);
        if(s3ImportSystem == null){
            cdlService.createS3ImportSystem(mainTestTenant.getId(), "ExternalSystem",
                    S3ImportSystem.SystemType.Other, false);
        }
        FetchFieldDefinitionsResponse  fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(systemName, DEFAULT_SYSTEM_TYPE,
                        EntityType.Contacts.name(), accountFile.getName());

        FieldDefinition crmID = new FieldDefinition();
        crmID.setColumnName("ID");
        crmID.setFieldName("SFDC ID");
        crmID.setFieldType(UserDefinedType.TEXT);
        crmID.setExternalSystemType(CDLExternalSystemType.CRM);

        FieldDefinition mapID = new FieldDefinition();
        mapID.setColumnName("ID");
        mapID.setFieldName("MAP_System");
        mapID.setFieldType(UserDefinedType.TEXT);
        mapID.setExternalSystemType(CDLExternalSystemType.MAP);
        fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord()
                .getFieldDefinitionsRecords(FieldDefinitionSectionName.Other_IDs.getName())
                .addAll(Arrays.asList(crmID, mapID));

        dataMappingService.commitFieldDefinitions(systemName, DEFAULT_SYSTEM_TYPE, EntityType.Contacts.name(),
                accountFile.getName(), false,
                fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord());

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, accountFile.getName(),
                accountFile.getName(), SOURCE, EntityType.Contacts.getEntity().name(), feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<Action> actions = actionProxy.getActions(customerSpace);
        validateImportAction(actions);
        validateJobsPage();

        DataFeedTask extrenalAccount = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType,
                EntityType.Contacts.getEntity().name());
        Assert.assertNotNull(extrenalAccount.getImportTemplate().getAttribute("user_SFDC_ID"));
        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace,
                EntityType.Contacts.getEntity().name());
        Assert.assertNotNull(system);
        Assert.assertTrue(system.getCRMIdList().contains("user_SFDC_ID"));
        Assert.assertEquals(system.getDisplayNameById("user_SFDC_ID"), "SFDC ID");
    }

    private void validateImportAction(List<Action> actions) {
        Assert.assertNotNull(actions);
        log.info(String.format("Actions are %s", Arrays.toString(actions.toArray())));
    }

    @SuppressWarnings("unchecked")
    private void validateJobsPage() {
        List<Object> listObj = restTemplate.getForObject( //
                String.format("%s/pls/jobs", getRestAPIHostPort()), List.class);
        List<Job> jobs = JsonUtils.convertList(listObj, Job.class);
        Assert.assertNotNull(jobs);
        log.info(String.format("jobs are %s", Arrays.toString(jobs.toArray())));
        Assert.assertTrue(jobs.size() >= 1);
    }

    @Test(groups = "deployment")
    public void testSchemaUpdate() throws Exception {
        SourceFile firstFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(),
                "Small_Account.csv", ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Small_Account.csv"));

        String systemName = "TestSchemaUpdate";
        String systemType = DEFAULT_SYSTEM_TYPE;
        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), systemName);
        if(s3ImportSystem == null) {
            cdlService.createS3ImportSystem(mainTestTenant.getId(), systemName,
                    S3ImportSystem.SystemType.Other, false);
        }
        String feedType = EntityTypeUtils.generateFullFeedType(systemName, EntityType.Accounts);
        boolean cityExist = false;
        boolean countryExist = false;
        FetchFieldDefinitionsResponse fetchResponse = dataMappingService.fetchFieldDefinitions(
                systemName, DEFAULT_SYSTEM_TYPE, EntityType.Accounts.name(), firstFile.getName());
        FieldDefinitionsRecord fieldRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                fieldRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Account_Matching_Fields.getName())) {
            if ("city".equalsIgnoreCase(definition.getColumnName())) {
                cityExist = true;
            }
            if ("country".equalsIgnoreCase(definition.getColumnName())) {
                countryExist = true;
            }
        }
        Assert.assertFalse(cityExist);
        Assert.assertFalse(countryExist);

        dataMappingService.commitFieldDefinitions(systemName, systemType,
                EntityType.Accounts.name(), firstFile.getName(), false, fieldRecord);

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, firstFile.getName(),
                firstFile.getName(), SOURCE, EntityType.Accounts.getEntity().name(), feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        SourceFile secondFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(),
                "Extend_Account.csv", ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Extend_Account.csv"));

        fetchResponse = dataMappingService.fetchFieldDefinitions(
                systemName, DEFAULT_SYSTEM_TYPE, EntityType.Accounts.name(), secondFile.getName());
        fieldRecord = fetchResponse.getCurrentFieldDefinitionsRecord();
        for (FieldDefinition definition :
                fieldRecord.getFieldDefinitionsRecords(FieldDefinitionSectionName.Account_Matching_Fields.getName())) {
            if ("city".equalsIgnoreCase(definition.getColumnName())) {
                Assert.assertNotNull(definition.getFieldName());
                cityExist = true;
            }
            if ("country".equalsIgnoreCase(definition.getColumnName())) {
                Assert.assertNotNull(definition.getFieldName());
                countryExist = true;
            }
        }
        Assert.assertTrue(cityExist);
        Assert.assertTrue(countryExist);
    }

    @Test(groups = "deployment")
    public void importBase() throws Exception {
        prepareBaseData(EntityType.Accounts);
        prepareBaseData(EntityType.Contacts);
        //prepareBaseData(ENTITY_TRANSACTION);
        getDataFeedTask(EntityType.Accounts);
        getDataFeedTask(EntityType.Contacts);
        //getDataFeedTask(ENTITY_TRANSACTION);
    }



    @Test(groups = "deployment", dependsOnMethods = "importBase", enabled = false)
    public void verifyTransaction() throws IOException {
        Assert.assertNotNull(baseTransactionFile);
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Transaction/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        String avroFileName = baseTransactionFile.getName().substring(0,
                baseTransactionFile.getName().lastIndexOf("."));
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath, file ->
                !file.isDirectory() && file.getPath().toString().contains(avroFileName)
                        && file.getPath().getName().endsWith("avro"));
        Assert.assertEquals(avroFiles.size(), 1);
        String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));
        long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");
        Assert.assertEquals(rowCount, 101);
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFiles.get(0)));
        Assert.assertEquals(schema.getField("TransactionTime").schema().getTypes().get(0).getType(),
                Schema.Type.STRING);
        Assert.assertEquals(schema.getField("Amount").schema().getTypes().get(0).getType(), Schema.Type.DOUBLE);
        Assert.assertEquals(schema.getField("Quantity").schema().getTypes().get(0).getType(), Schema.Type.DOUBLE);
    }

    @Test(groups = "deployment", enabled = false)
    public void verifyRequiredFieldMissing() throws Exception {
        SourceFile missingColumn = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE,
                EntityType.ProductPurchases, TRANSACTION_SOURCE_FILE_MISSING);
        Assert.assertNotNull(missingColumn);
        Exception exp = null;
        try {
            startCDLImport(missingColumn, EntityType.ProductPurchases);
        } catch (RuntimeException e) {
            exp = e;
        }
        Assert.assertNotNull(exp);
    }


    @Test(groups = "deployment", dependsOnMethods = "importBase")
    public void verifyBase() {
        Assert.assertNotNull(accountDataFeedTask);
        Assert.assertNotNull(contactDataFeedTask);
        Table accountTemplate = accountDataFeedTask.getImportTemplate();
        Table contactTemplate = contactDataFeedTask.getImportTemplate();
        //Table transactionTemplate = transactionDataFeedTask.getImportTemplate();
        Set<String> accountHeaders = getHeaderFields(
                ClassLoader.getSystemResource(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        Table accountSourceTable = metadataProxy.getTable(customerSpace, baseAccountFile.getTableName());
        compare(accountSourceTable, accountHeaders);
        Assert.assertEquals(accountTemplate.getAttributes().size(), accountSourceTable.getAttributes().size());
        Assert.assertNotNull(contactTemplate.getAttribute(InterfaceName.PhoneNumber));
        //boolean containsContact =
        //        transactionTemplate.getAttribute(InterfaceName.ContactId) != null ^
        //                transactionTemplate.getAttribute(InterfaceName.CustomerContactId) != null;
        //Assert.assertTrue(containsContact);
        List<Action> importActions = actionProxy.getActions(customerSpace).stream()
                .filter(action -> ActionType.CDL_DATAFEED_IMPORT_WORKFLOW.equals(action.getType()))
                .collect(Collectors.toList());
        for (Action action : importActions) {
            Assert.assertNotNull(action.getActionConfiguration());
            ImportActionConfiguration importActionConfiguration = (ImportActionConfiguration) action.getActionConfiguration();
            Assert.assertNotNull(importActionConfiguration.getOriginalFilename());
        }
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyBase")
    public void verifyColumnMissing() throws Exception {
        SourceFile missingAccountFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE,
                EntityType.Accounts, ACCOUNT_SOURCE_FILE_MISSING);
        Assert.assertNotNull(missingAccountFile);
        startCDLImport(missingAccountFile, EntityType.Accounts);
        accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts), EntityType.Accounts.getEntity().name());
        Table accountTemplate2 = accountDataFeedTask.getImportTemplate();
        Table sourceTable = metadataProxy.getTable(customerSpace, missingAccountFile.getTableName());
        // in new version code the template contains the existing table attributes, the assertion will be invalidate
        //Assert.assertNull(sourceTable.getAttribute(InterfaceName.Website));
        Assert.assertNotNull(accountTemplate2.getAttribute(InterfaceName.Website));
    }



    private Set<String> getHeaderFields(URL sourceFileURL) {
        try {
            CloseableResourcePool closeableResourcePool = new CloseableResourcePool();
            InputStream stream = new FileInputStream(new File(sourceFileURL.getFile()));

            return ValidateFileHeaderUtils.getCSVHeaderFields(stream, closeableResourcePool);
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
    }

    private void compare(Table table, Set<String> headers) {
        List<Attribute> attributes = table.getAttributes();
        assertTrue(attributes.size() >= headers.size());
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyColumnMissing", enabled = false)
    public void testParallelImport() throws Exception {
        SourceFile sourceFile1 = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Contacts,
                ACCOUNT_SOURCE_FILE_MISSING);
        Assert.assertNotNull(sourceFile1);
        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts);
        ApplicationId applicationId1 = cdlService.submitCSVImport(customerSpace, sourceFile1.getName(),
                sourceFile1.getName(), SOURCE, EntityType.Accounts.getEntity().name(), feedType);

        SourceFile sourceFile2 = uploadSourceFile(DEFAULT_SYSTEM,
                DEFAULT_SYSTEM_TYPE, EntityType.Accounts, ACCOUNT_SOURCE_FILE);
        Assert.assertNotNull(sourceFile2);
        ApplicationId applicationId2 = cdlService.submitCSVImport(customerSpace, sourceFile2.getName(),
                sourceFile2.getName(), SOURCE, EntityType.Accounts.getEntity().name(), feedType);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                feedType, EntityType.Accounts.getEntity().name());
        Assert.assertNotNull(dataFeedTask);
        JobStatus completedStatus1 = waitForWorkflowStatus(workflowProxy, applicationId1.toString(), false);
        assertEquals(completedStatus1, JobStatus.COMPLETED);
        JobStatus completedStatus2 = waitForWorkflowStatus(workflowProxy, applicationId2.toString(), false);
        assertEquals(completedStatus2, JobStatus.COMPLETED);
    }

    // Backend will add some auto update logic to prevent wrong mapping.
    @Test(groups = "deployment", enabled = false)
    public void testWrongFieldMapping() throws Exception {
        SourceFile firstFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                EntityType.Accounts.getSchemaInterpretation(), EntityType.Accounts.getEntity().name(), "Small_Account.csv",
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Small_Account.csv"));

        String feedType = EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, EntityType.Accounts);

        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE,
                        EntityType.Accounts.getDisplayName(), firstFile.getName());

        FieldDefinition m1 = new FieldDefinition();
        m1.setColumnName("ID");
        m1.setFieldName("AccountId");
        m1.setExternalSystemType(CDLExternalSystemType.CRM);

        FieldDefinitionsRecord currentRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord()
                .getFieldDefinitionsRecords(FieldDefinitionSectionName.Other_IDs.getName()).add(m1);

        dataMappingService.commitFieldDefinitions(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE,
                EntityType.Accounts.getDisplayName(), firstFile.getName(), false, currentRecord);

        boolean submitError = false;
        try {
            cdlService.submitCSVImport(customerSpace, firstFile.getName(), firstFile.getName(), SOURCE,
                    EntityType.Accounts.getEntity().name(),
                    feedType);
        } catch (Exception e) {
            submitError = true;
        }
        Assert.assertTrue(submitError, "There should be error when submit wrong field mapping jobs.");
    }

}
