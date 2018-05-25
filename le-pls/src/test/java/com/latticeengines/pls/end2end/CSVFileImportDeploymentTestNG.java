package com.latticeengines.pls.end2end;

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

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
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
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.ValidateFileHeaderUtils;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class CSVFileImportDeploymentTestNG extends CDLDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(CSVFileImportDeploymentTestNG.class);

    private static final String SOURCE_FILE_LOCAL_PATH = "com/latticeengines/pls/end2end/cdlCSVImport/";
    private static final String SOURCE = "File";
    private static final String FEED_TYPE_SUFFIX = "Schema";

    private static final String ENTITY_ACCOUNT = "Account";
    private static final String ENTITY_CONTACT = "Contact";
    private static final String ENTITY_TRANSACTION = "Transaction";

    private static final String ACCOUNT_SOURCE_FILE = "Account_base.csv";
    private static final String CONTACT_SOURCE_FILE = "Contact_base.csv";
    private static final String TRANSACTION_SOURCE_FILE = "Transaction_base.csv";

    private static final String ACCOUNT_SOURCE_FILE_MISSING = "Account_missing_Website.csv";
    private static final String TRANSACTION_SOURCE_FILE_MISSING = "Transaction_missing_required.csv";

    @Autowired
    private ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    private FileUploadService fileUploadService;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private WorkflowProxy workflowProxy;

    @Autowired
    private CDLService cdlService;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Inject
    private ActionProxy actionProxy;

    @Autowired
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @Autowired
    private Configuration yarnConfiguration;

    private SourceFile baseAccountFile;

    private SourceFile baseContactFile;

    private SourceFile baseTransactionFile;

    private SourceFile missingAccountFile;

    private DataFeedTask accountDataFeedTask;

    private DataFeedTask contactDataFeedTask;

    private DataFeedTask transactionDataFeedTask;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.CG);
        MultiTenantContext.setTenant(mainTestTenant);
        customerSpace = CustomerSpace.parse(mainTestTenant.getId()).toString();
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
        Assert.assertNotNull(extrenalAccount.getImportTemplate().getAttribute("SFDC_ID"));
        CDLExternalSystem system = cdlExternalSystemProxy.getCDLExternalSystem(customerSpace);
        Assert.assertNotNull(system);
        Assert.assertTrue(system.getCRMIdList().contains("SFDC_ID"));
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

    private void prepareBaseData(String entity) {
        switch (entity) {
        case ENTITY_ACCOUNT:
            baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
            Assert.assertNotNull(baseAccountFile);
            startCDLImport(baseAccountFile, ENTITY_ACCOUNT);
            break;
        case ENTITY_CONTACT:
            baseContactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
            Assert.assertNotNull(baseContactFile);
            startCDLImport(baseContactFile, ENTITY_CONTACT);
            break;
        case ENTITY_TRANSACTION:
            baseTransactionFile = uploadSourceFile(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION);
            Assert.assertNotNull(baseTransactionFile);
            startCDLImport(baseTransactionFile, ENTITY_TRANSACTION);
            break;
        }
    }

    private void getDataFeedTask(String entity) {
        switch (entity) {
        case ENTITY_ACCOUNT:
            accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                    ENTITY_ACCOUNT + FEED_TYPE_SUFFIX, ENTITY_ACCOUNT);
            break;
        case ENTITY_CONTACT:
            contactDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                    ENTITY_CONTACT + FEED_TYPE_SUFFIX, ENTITY_CONTACT);
            break;
        case ENTITY_TRANSACTION:
            transactionDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                    ENTITY_TRANSACTION + FEED_TYPE_SUFFIX, ENTITY_TRANSACTION);
            break;
        }
    }

    private SourceFile uploadSourceFile(String csvFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));

        String feedType = entity + FEED_TYPE_SUFFIX;
        FieldMappingDocument fieldMappingDocument = modelingFileMetadataService
                .getFieldMappingDocumentBestEffort(sourceFile.getName(), entity, SOURCE, feedType);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        modelingFileMetadataService.resolveMetadata(sourceFile.getName(), fieldMappingDocument, entity, SOURCE,
                feedType);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        return sourceFile;
    }

    private void startCDLImport(SourceFile sourceFile, String entity) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, entity + FEED_TYPE_SUFFIX);

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
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
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, targetPath, file -> {
            if (!file.isDirectory() && file.getPath().toString().contains(avroFileName)
                    && file.getPath().getName().endsWith("avro")) {
                return true;
            }
            return false;
        });
        Assert.assertEquals(avroFiles.size(), 1);
        String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));
        long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");
        Assert.assertEquals(rowCount, 101);
        Schema schema = AvroUtils.getSchema(yarnConfiguration, new Path(avroFiles.get(0)));
        Assert.assertEquals(schema.getField("TransactionTime").schema().getTypes().get(0).getType(),
                Schema.Type.STRING);
        Assert.assertEquals(schema.getField("Amount").schema().getTypes().get(0).getType(), Schema.Type.INT);
        Assert.assertEquals(schema.getField("Quantity").schema().getTypes().get(0).getType(), Schema.Type.INT);
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
        missingAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE_MISSING, ENTITY_ACCOUNT);
        Assert.assertNotNull(missingAccountFile);
        startCDLImport(missingAccountFile, ENTITY_ACCOUNT);
        accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE, ENTITY_ACCOUNT + FEED_TYPE_SUFFIX,
                ENTITY_ACCOUNT);
        Table accountTemplate2 = accountDataFeedTask.getImportTemplate();
        Table sourceTable = metadataProxy.getTable(customerSpace, missingAccountFile.getTableName());
        Assert.assertNotNull(sourceTable.getAttribute(InterfaceName.Website));
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
        Assert.assertNotNull(ex);
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

    @Test(groups = "deployment")
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
            cdlService.submitCSVImport(customerSpace, firstFile.getName(),
                    firstFile.getName(), SOURCE, ENTITY_ACCOUNT, feedType);
        } catch (Exception e) {
            submitError = true;
        }
        Assert.assertTrue(submitError, "There should be error when submit wrong field mapping jobs.");
    }
}