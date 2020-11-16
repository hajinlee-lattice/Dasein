package com.latticeengines.pls.end2end.fileimport;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.testng.Assert;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public abstract class CSVFileImportDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    protected static final String SOURCE_FILE_LOCAL_PATH = "com/latticeengines/pls/end2end/cdlCSVImport/";
    protected static final String SOURCE = "File";
    protected static final String FEED_TYPE_SUFFIX = "Schema";
    protected static final String DEFAULT_SYSTEM = "DefaultSystem";
    protected static final String SPLIT_CHART = "_";

    protected static final String ENTITY_ACCOUNT = "Account";
    protected static final String ENTITY_CONTACT = "Contact";
    protected static final String ENTITY_TRANSACTION = "Transaction";
    protected static final String ENTITY_PRODUCT = "Product";
    protected static final String ENTITY_ACTIVITY_STREAM = "ActivityStream";
    protected static final String ENTITY_CATALOG = "Catalog";

    protected static final String ACCOUNT_SOURCE_FILE = "Account_base.csv";
    protected static final String CONTACT_SOURCE_FILE = "Contact_base.csv";
    protected static final String TRANSACTION_SOURCE_FILE = "Transaction_base.csv";
    protected static final String PRODUCT_SOURCE_FILE = "Product_Bundles.csv";

    protected static final String ACCOUNT_SOURCE_FILE_FROMATDATE = "Account_FormatDate.csv";

    protected static final String ACCOUNT_SOURCE_FILE_MISSING = "Account_missing_Website.csv";
    protected static final String TRANSACTION_SOURCE_FILE_MISSING = "Transaction_missing_required.csv";

    private static final String DEFAULT_WEBSITE_SYSTEM = "Default_Website_System";

    @Inject
    protected FileUploadService fileUploadService;

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    protected ModelingFileMetadataService modelingFileMetadataService;

    @Inject
    protected WorkflowProxy workflowProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLProxy cdlProxy;

    @Inject
    protected CDLService cdlService;

    @Inject
    protected Configuration yarnConfiguration;

    protected SourceFile baseAccountFile;

    protected SourceFile baseContactFile;

    protected SourceFile baseTransactionFile;

    protected DataFeedTask accountDataFeedTask;

    protected DataFeedTask contactDataFeedTask;

    protected DataFeedTask transactionDataFeedTask;

    protected DataFeedTask webVisitDataFeedTask;

    protected DataFeedTask webVisitPathPatternDataFeedTask;

    protected void prepareBaseData(String entity) {
        switch (entity) {
            case ENTITY_ACCOUNT:
                baseAccountFile = uploadSourceFile(ACCOUNT_SOURCE_FILE, ENTITY_ACCOUNT);
                Assert.assertNotNull(baseAccountFile);
                startCDLImport(baseAccountFile, ENTITY_ACCOUNT, DEFAULT_SYSTEM);
                break;
            case ENTITY_CONTACT:
                baseContactFile = uploadSourceFile(CONTACT_SOURCE_FILE, ENTITY_CONTACT);
                Assert.assertNotNull(baseContactFile);
                startCDLImport(baseContactFile, ENTITY_CONTACT, DEFAULT_SYSTEM);
                break;
            case ENTITY_TRANSACTION:
                baseTransactionFile = uploadSourceFile(TRANSACTION_SOURCE_FILE, ENTITY_TRANSACTION);
                Assert.assertNotNull(baseTransactionFile);
                startCDLImport(baseTransactionFile, ENTITY_TRANSACTION, DEFAULT_SYSTEM);
                break;
            default:
        }
    }

    protected void getDataFeedTask(String entity) {
        switch (entity) {
            case ENTITY_ACCOUNT:
                accountDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                        DEFAULT_SYSTEM + SPLIT_CHART + EntityType.Accounts.getDefaultFeedTypeName(), ENTITY_ACCOUNT);
                break;
            case ENTITY_CONTACT:
                contactDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                        DEFAULT_SYSTEM + SPLIT_CHART + EntityType.Contacts.getDefaultFeedTypeName(), ENTITY_CONTACT);
                break;
            case ENTITY_TRANSACTION:
                transactionDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                        DEFAULT_SYSTEM + SPLIT_CHART + EntityType.ProductPurchases.getDefaultFeedTypeName(), ENTITY_TRANSACTION);
                break;
            case ENTITY_ACTIVITY_STREAM:
                webVisitDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                        DEFAULT_WEBSITE_SYSTEM + SPLIT_CHART + EntityType.WebVisit.getDefaultFeedTypeName(), ENTITY_ACTIVITY_STREAM);
                break;
            case ENTITY_CATALOG:
                webVisitPathPatternDataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                        DEFAULT_WEBSITE_SYSTEM + SPLIT_CHART + EntityType.WebVisitPathPattern.getDefaultFeedTypeName(), ENTITY_CATALOG);
                break;
            default:
        }
    }

    protected SourceFile uploadSourceFile(String csvFileName, String entity) {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                SchemaInterpretation.valueOf(entity), entity, csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));

        String feedType = getFeedTypeByEntity(DEFAULT_SYSTEM, entity);
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

    protected void startCDLImport(SourceFile sourceFile, String entity, String system) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(system, entity));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    public String getFeedTypeByEntity(String system, String entity) {
        String feedType = entity + FEED_TYPE_SUFFIX;
        switch (entity) {
            case ENTITY_ACCOUNT:
                feedType = system + SPLIT_CHART + EntityType.Accounts.getDefaultFeedTypeName();
                break;
            case ENTITY_CONTACT:
                feedType = system + SPLIT_CHART + EntityType.Contacts.getDefaultFeedTypeName();
                break;
            case ENTITY_TRANSACTION:
                feedType = system + SPLIT_CHART + EntityType.ProductPurchases.getDefaultFeedTypeName();
                break;
            default:
                break;
        }
        return feedType;
    }

    protected void verifyAvroFileNumber(SourceFile sourceFile, int num, String path)
            throws IOException {
        String avroFileName = sourceFile.getName().substring(0, sourceFile.getName().lastIndexOf("."));
        List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, path, file -> !file.isDirectory()
                && file.getPath().toString().contains(avroFileName) && file.getPath().getName().endsWith("avro"));
        Assert.assertEquals(avroFiles.size(), 1);
        String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));
        long rowCount = AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");

        Assert.assertEquals(rowCount, num);
    }

    protected void verifyFailed(SourceFile sourceFile, String entity) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(DEFAULT_SYSTEM, entity));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.FAILED);
    }

    protected void verifyReport(Report report1, long ignored, long failed, long imported) {
        Report report = restTemplate.getForObject(getRestAPIHostPort() + String.format("/pls/reports/%s",
                report1.getName()), Report.class);
        Assert.assertNotNull(report.getJson());
        ObjectNode node = JsonUtils.deserialize(report.getJson().getPayload(), ObjectNode.class);
        Assert.assertEquals(node.get("ignored_rows").longValue(), ignored);
        Assert.assertEquals(node.get("total_failed_rows").longValue(), failed);
        Assert.assertEquals(node.get("imported_rows").longValue(), imported);
    }

    protected void verifyEaiJobDetail(EaiImportJobDetail detail, Long ignored, int processed) {
        Assert.assertEquals(detail.getIgnoredRows(), ignored);
        Assert.assertEquals(detail.getProcessedRecords(), processed);

    }

    protected void createDefaultImportSystem() {
        S3ImportSystem importSystem = new S3ImportSystem();
        importSystem.setPriority(1);
        importSystem.setName("DefaultSystem");
        importSystem.setDisplayName("DefaultSystem");
        importSystem.setSystemType(S3ImportSystem.SystemType.Other);
        importSystem.setTenant(mainTestTenant);
        cdlProxy.createS3ImportSystem(customerSpace, importSystem);
    }
}
