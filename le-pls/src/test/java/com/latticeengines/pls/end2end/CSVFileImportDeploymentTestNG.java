package com.latticeengines.pls.end2end;

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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystem;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DropBoxSummary;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.S3ImportTemplateDisplay;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.util.S3PathBuilder;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DropBoxProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

public class CSVFileImportDeploymentTestNG extends CSVFileImportDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportDeploymentTestNG.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private DropBoxProxy dropBoxProxy;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    private List<S3ImportTemplateDisplay> templates = null;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
        templates = cdlService.getS3ImportTemplate(customerSpace);
    }

    @Test(groups = "deployment")
    public void testExternalSystem() {
        SourceFile accountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        String feedType = ENTITY_ACCOUNT + FEED_TYPE_SUFFIX + "ExternalSystem";
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);

        FieldMapping crmID = new FieldMapping();
        crmID.setUserField("ID");
        crmID.setMappedField("SFDC ID");
        crmID.setFieldType(UserDefinedType.TEXT);
        crmID.setMappedToLatticeField(false);
        crmID.setCdlExternalSystemType(CDLExternalSystemType.CRM);

        FieldMapping mapID = new FieldMapping();
        mapID.setUserField("ID");
        mapID.setMappedField("MAP_System");
        mapID.setFieldType(UserDefinedType.TEXT);
        mapID.setMappedToLatticeField(false);
        mapID.setCdlExternalSystemType(CDLExternalSystemType.MAP);
        fieldMappingDocument.getFieldMappings().addAll(Arrays.asList(crmID, mapID));

        modelingFileMetadataService.resolveMetadata(accountFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, accountFile.getName(),
                accountFile.getName(), SOURCE, ENTITY_ACCOUNT, feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<Action> actions = actionProxy.getActions(customerSpace);
        validateImportAction(actions);
        validateJobsPage();

        DataFeedTask extrenalAccount = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType, ENTITY_ACCOUNT);
        Assert.assertNotNull(extrenalAccount.getImportTemplate().getAttribute("user_SFDC_ID"));
        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace, ENTITY_ACCOUNT);
        Assert.assertNotNull(system);
        Assert.assertTrue(system.getCRMIdList().contains("user_SFDC_ID"));
        Assert.assertEquals(system.getDisplayNameById("user_SFDC_ID"), "SFDC ID");
    }

    @Test(groups = "deployment")
    public void testFormatDate() throws Exception {
        SourceFile accountFile = fileUploadService.uploadFile(
                "file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, ACCOUNT_SOURCE_FILE_FROMATDATE,
                ClassLoader
                        .getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE_FROMATDATE));
        String feedType = ENTITY_ACCOUNT + FEED_TYPE_SUFFIX + "FormatDate";
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountFile.getName(), ENTITY_ACCOUNT, SOURCE,
                        feedType);

        String dateFormatString1 = "DD/MM/YYYY";
        String timeFormatString1 = null;
        String timezone1 = "America/New_York";
        String dateFormatString2 = "MM.DD.YY";
        String timeFormatString2 = "00:00:00 24H";
        String timezone2 = "Asia/Shanghai";
        for (FieldMapping mapping : fieldMappingDocument.getFieldMappings()) {
            if (mapping.getUserField().equals("TestDate1")) {
                mapping.setFieldType(UserDefinedType.DATE);
                mapping.setMappedToLatticeField(false);
                mapping.setDateFormatString(dateFormatString1);
                mapping.setTimeFormatString(timeFormatString1);
                mapping.setTimezone(timezone1);
            } else if (mapping.getUserField().equals("TestDate2")) {
                mapping.setFieldType(UserDefinedType.DATE);
                mapping.setMappedToLatticeField(false);
                mapping.setDateFormatString(dateFormatString2);
                mapping.setTimeFormatString(timeFormatString2);
                mapping.setTimezone(timezone2);
            }
        }

        modelingFileMetadataService.resolveMetadata(accountFile.getName(), fieldMappingDocument,
                ENTITY_ACCOUNT, SOURCE, feedType);

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace,
                accountFile.getName(), accountFile.getName(), SOURCE, ENTITY_ACCOUNT, feedType);

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
        Assert.assertEquals(rowCount, 50);

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
        long expected2b = computeTimestamp("12.31.18 23:59:59", true,
                "M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2b, 1546271999000L);
        Assert.assertEquals(records.get(1).get(fieldName2).toString(), Long.toString(expected2b));
        long expected2c = computeTimestamp("5.6.99 12:34:56", true,
                "M.d.yy H:m:s", "GMT+8");
        Assert.assertEquals(expected2c, 4081725296000L);
        Assert.assertEquals(records.get(2).get(fieldName2).toString(), Long.toString(expected2c));
    }

    @Test(groups = "deployment")
    public void testExternalSystemWithContact() {
        SourceFile accountFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));
        String feedType = ENTITY_CONTACT + FEED_TYPE_SUFFIX + "ExternalSystem";
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(accountFile.getName(), ENTITY_CONTACT, SOURCE, feedType);

        FieldMapping crmID = new FieldMapping();
        crmID.setUserField("ID");
        crmID.setMappedField("SFDC ID");
        crmID.setFieldType(UserDefinedType.TEXT);
        crmID.setMappedToLatticeField(false);
        crmID.setCdlExternalSystemType(CDLExternalSystemType.CRM);

        FieldMapping mapID = new FieldMapping();
        mapID.setUserField("ID");
        mapID.setMappedField("MAP_System");
        mapID.setFieldType(UserDefinedType.TEXT);
        mapID.setMappedToLatticeField(false);
        mapID.setCdlExternalSystemType(CDLExternalSystemType.MAP);
        fieldMappingDocument.getFieldMappings().addAll(Arrays.asList(crmID, mapID));

        modelingFileMetadataService.resolveMetadata(accountFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, accountFile.getName(),
                accountFile.getName(), SOURCE, ENTITY_CONTACT, feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        List<Action> actions = actionProxy.getActions(customerSpace);
        validateImportAction(actions);
        validateJobsPage();

        DataFeedTask extrenalAccount = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, feedType, ENTITY_CONTACT);
        Assert.assertNotNull(extrenalAccount.getImportTemplate().getAttribute("user_SFDC_ID"));
        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace, ENTITY_CONTACT);
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
    public void testSchemaUpdate() {
        SourceFile firstFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, "Small_Account.csv",
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Small_Account.csv"));

        String feedType = ENTITY_ACCOUNT + FEED_TYPE_SUFFIX + "TestSchemaUpdate";
        boolean cityExist = false;
        boolean countryExist = false;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(firstFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equalsIgnoreCase("city")) {
                cityExist = true;
            }
            if (fieldMapping.getUserField().equalsIgnoreCase("country")) {
                countryExist = true;
            }
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        Assert.assertFalse(cityExist);
        Assert.assertFalse(countryExist);

        modelingFileMetadataService.resolveMetadata(firstFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);

        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, firstFile.getName(),
                firstFile.getName(), SOURCE, ENTITY_ACCOUNT, feedType);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

        SourceFile secondFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, "Extend_Account.csv",
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Extend_Account.csv"));

        fieldMappingDocument = modelingFileMetadataService.getFieldMappingDocumentBestEffort(secondFile.getName(),
                ENTITY_ACCOUNT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getUserField().equalsIgnoreCase("city")) {
                Assert.assertNotNull(fieldMapping.getMappedField());
                cityExist = true;
            }
            if (fieldMapping.getUserField().equalsIgnoreCase("country")) {
                Assert.assertNotNull(fieldMapping.getMappedField());
                countryExist = true;
            }
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        Assert.assertTrue(cityExist);
        Assert.assertTrue(countryExist);
    }

    @Test(groups = "deployment")
    public void importBase() {
        prepareBaseData(ENTITY_ACCOUNT);
        prepareBaseData(ENTITY_CONTACT);
        prepareBaseData(ENTITY_TRANSACTION);
        getDataFeedTask(ENTITY_ACCOUNT);
        getDataFeedTask(ENTITY_CONTACT);
        getDataFeedTask(ENTITY_TRANSACTION);
    }



    @Test(groups = "deployment", dependsOnMethods = "importBase")
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

    @Test(groups = "deployment")
    public void verifyRequiredFieldMissing() {
        SourceFile missingColumn = uploadSourceFile(TRANSACTION_SOURCE_FILE_MISSING, ENTITY_TRANSACTION);
        Assert.assertNotNull(missingColumn);
        Exception exp = null;
        try {
            startCDLImport(missingColumn, ENTITY_TRANSACTION);
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
        Table transactionTemplate = transactionDataFeedTask.getImportTemplate();
        Set<String> accountHeaders = getHeaderFields(
                ClassLoader.getSystemResource(SOURCE_FILE_LOCAL_PATH + ACCOUNT_SOURCE_FILE));
        Table accountSourceTable = metadataProxy.getTable(customerSpace, baseAccountFile.getTableName());
        compare(accountSourceTable, accountHeaders);
        Assert.assertEquals(accountTemplate.getAttributes().size(), accountSourceTable.getAttributes().size());
        Assert.assertNotNull(contactTemplate.getAttribute(InterfaceName.PhoneNumber));
        Assert.assertNotNull(transactionTemplate.getAttribute(InterfaceName.ContactId));
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyBase")
    public void verifyColumnMissing() {
        SourceFile missingAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE_MISSING, ENTITY_ACCOUNT);
        Assert.assertNotNull(missingAccountFile);
        startCDLImport(missingAccountFile, ENTITY_ACCOUNT);
        accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX,
                ENTITY_ACCOUNT);
        Table accountTemplate2 = accountDataFeedTask.getImportTemplate();
        Table sourceTable = metadataProxy.getTable(customerSpace, missingAccountFile.getTableName());
        Assert.assertNull(sourceTable.getAttribute(InterfaceName.Website));
        Assert.assertNotNull(accountTemplate2.getAttribute(InterfaceName.Website));
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyBase")
    public void verifyDataTypeChange() {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_CONTACT), ENTITY_CONTACT, CONTACT_SOURCE_FILE,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + CONTACT_SOURCE_FILE));

        String feedType = ENTITY_CONTACT + FEED_TYPE_SUFFIX;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), ENTITY_CONTACT, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equalsIgnoreCase("Fax")) {
                fieldMapping.setFieldType(UserDefinedType.NUMBER);
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, ENTITY_CONTACT, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());
        Exception ex = null;
        try {
            ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                    sourceFile.getName(), SOURCE, ENTITY_CONTACT, ENTITY_CONTACT + FEED_TYPE_SUFFIX);

            JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
            assertEquals(completedStatus, JobStatus.COMPLETED);
        } catch (Exception e) {
            ex = e;
        }
        Assert.assertNull(ex);
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

    @Test(groups = "deployment", dependsOnMethods = "verifyColumnMissing")
    public void testParallelImport() {
        SourceFile sourceFile1 = uploadSourceFile(ACCOUNT_SOURCE_FILE_MISSING, ENTITY_ACCOUNT);
        Assert.assertNotNull(sourceFile1);
        ApplicationId applicationId1 = cdlService.submitCSVImport(customerSpace, sourceFile1.getName(),
                sourceFile1.getName(), SOURCE, ENTITY_ACCOUNT, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX);

        SourceFile sourceFile2 = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
        Assert.assertNotNull(sourceFile2);
        ApplicationId applicationId2 = cdlService.submitCSVImport(customerSpace, sourceFile2.getName(),
                sourceFile2.getName(), SOURCE, ENTITY_ACCOUNT, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX);
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT);
        Assert.assertNotNull(dataFeedTask);
        JobStatus completedStatus1 = waitForWorkflowStatus(workflowProxy, applicationId1.toString(), false);
        assertEquals(completedStatus1, JobStatus.COMPLETED);
        JobStatus completedStatus2 = waitForWorkflowStatus(workflowProxy, applicationId2.toString(), false);
        assertEquals(completedStatus2, JobStatus.COMPLETED);
    }

    // Backend will add some auto update logic to prevent wrong mapping.
    @Test(groups = "deployment", enabled = false)
    public void testWrongFieldMapping() {
        SourceFile firstFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(ENTITY_ACCOUNT), ENTITY_ACCOUNT, "Small_Account.csv",
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + "Small_Account.csv"));

        String feedType = ENTITY_ACCOUNT + FEED_TYPE_SUFFIX + "TestWrongFieldMapping";

        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(firstFile.getName(), ENTITY_ACCOUNT, SOURCE, feedType);

        FieldMapping m1 = new FieldMapping();
        m1.setUserField("ID");
        m1.setMappedToLatticeField(false);
        m1.setMappedField("AccountId");
        m1.setCdlExternalSystemType(CDLExternalSystemType.CRM);

        fieldMappingDocument.getFieldMappings().add(m1);

        modelingFileMetadataService.resolveMetadata(firstFile.getName(), fieldMappingDocument, ENTITY_ACCOUNT, SOURCE,
                feedType);

        boolean submitError = false;
        try {
            cdlService.submitCSVImport(customerSpace, firstFile.getName(), firstFile.getName(), SOURCE, ENTITY_ACCOUNT,
                    feedType);
        } catch (Exception e) {
            submitError = true;
        }
        Assert.assertTrue(submitError, "There should be error when submit wrong field mapping jobs.");
    }

    @Test(groups = "deployment", dependsOnMethods = "verifyTransaction")
    public void importS3Base() {
        prepareS3BaseData(ENTITY_ACCOUNT, EntityType.Accounts);
        prepareS3BaseData(ENTITY_CONTACT, EntityType.Contacts);
        prepareS3BaseData(ENTITY_TRANSACTION, EntityType.ProductPurchases);
    }

    private void prepareS3BaseData(String entity, EntityType entityType) {
        switch (entity) {
        case ENTITY_ACCOUNT:
            testS3ImportWithTemplateData(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT, entityType);
            testS3ImportOnlyData(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
            break;
        case ENTITY_CONTACT:
            testS3ImportWithTemplateData(CONTACT_SOURCE_FILE, ENTITY_CONTACT, entityType);
            testS3ImportOnlyData(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
            break;
        case ENTITY_TRANSACTION:
            testS3ImportWithTemplateData(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION, entityType);
            testS3ImportOnlyData(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION);
            break;
        }
    }

    private void testS3ImportWithTemplateData(String csvFileName, String entity, EntityType entityType) {
        SourceFile sourceFile = uploadSourceFile(csvFileName, entity);
        String subType = entityType.getSubType() != null ? entityType.getSubType().name() : null;
        String taskId = cdlService.createS3Template(customerSpace, sourceFile.getName(), SOURCE, entity,
                entity + FEED_TYPE_SUFFIX, subType, entityType.getDisplayName());
        ApplicationId applicationId = cdlService.submitS3ImportWithTemplateData(customerSpace, taskId,
                sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);

    }

    private void testS3ImportOnlyData(String csvFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                entity + FEED_TYPE_SUFFIX);
        ApplicationId applicationId = cdlService.submitS3ImportOnlyData(customerSpace,
                dataFeedTask.getUniqueId(), sourceFile.getName());
        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @Test(groups = "deployment", dependsOnMethods = "importS3Base")
    public void testGetS3ImportDisplay() {
        // verify that the tenant has 5 template display by default
        Assert.assertNotNull(templates);
        Assert.assertEquals(templates.size(), 5);
        // S3ImportTemplateDisplay display = templates.get(0);
        // Assert.assertEquals(display.getPath(), "N/A");
        for (S3ImportTemplateDisplay display : templates) {
            Assert.assertEquals(display.getPath(), "N/A");
            Assert.assertEquals(display.getExist(), Boolean.FALSE);
        }
        templates = cdlService.getS3ImportTemplate(customerSpace);
        Assert.assertNotNull(templates);
        List<String> feedTypes = Arrays.asList(ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_CONTACT + FEED_TYPE_SUFFIX,
                ENTITY_TRANSACTION + FEED_TYPE_SUFFIX);
        List<S3ImportTemplateDisplay> renderedTemplats = templates.stream()
                .filter(template -> feedTypes.contains(template.getFeedType())).collect(Collectors.toList());
        for (S3ImportTemplateDisplay display : renderedTemplats) {
            DropBoxSummary dropBoxSummary = dropBoxProxy.getDropBox(customerSpace);
            Assert.assertNotNull(dropBoxSummary);
            Assert.assertEquals(display.getPath(), S3PathBuilder.getUiDisplayS3Dir(dropBoxSummary.getBucket(),
                    dropBoxSummary.getDropBox(), display.getFeedType()));
            Assert.assertEquals(display.getExist(), Boolean.TRUE);
        }
    }


}
