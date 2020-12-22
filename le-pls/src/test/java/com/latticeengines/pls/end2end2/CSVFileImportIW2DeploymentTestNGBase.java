package com.latticeengines.pls.end2end2;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.List;

import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FetchFieldDefinitionsResponse;
import com.latticeengines.domain.exposed.pls.frontend.FieldDefinitionsRecord;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.query.EntityTypeUtils;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.DataMappingService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public abstract class CSVFileImportIW2DeploymentTestNGBase extends CDLDeploymentTestNGBase {

    protected static final String SOURCE_FILE_LOCAL_PATH = "com/latticeengines/pls/end2end/cdlCSVImport/";
    protected static final String SOURCE = "File";
    protected static final String DEFAULT_SYSTEM = "DefaultSystem";

    protected static final String ACCOUNT_SOURCE_FILE = "Account_base.csv";
    protected static final String CONTACT_SOURCE_FILE = "Contact_base.csv";
    protected static final String TRANSACTION_SOURCE_FILE = "Transaction_base.csv";
    protected static final String PRODUCT_SOURCE_FILE = "Product_Bundles.csv";

    protected static final String ACCOUNT_SOURCE_FILE_FROMATDATE = "Account_FormatDate.csv";

    protected static final String ACCOUNT_SOURCE_FILE_MISSING = "Account_missing_Website.csv";
    protected static final String TRANSACTION_SOURCE_FILE_MISSING = "Transaction_missing_required.csv";

    protected static final String DEFAULT_SYSTEM_TYPE = S3ImportSystem.SystemType.Other.name();

    @Inject
    protected FileUploadService fileUploadService;

    @Inject
    protected SourceFileService sourceFileService;

    @Inject
    protected DataMappingService dataMappingService;

    @Inject
    protected ModelingFileMetadataService modelingFileMetadataService;

    @Inject
    protected WorkflowProxy workflowProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected CDLService cdlService;

    @Autowired
    protected Configuration yarnConfiguration;

    protected SourceFile baseAccountFile;

    protected SourceFile baseContactFile;

    protected SourceFile baseTransactionFile;

    protected DataFeedTask accountDataFeedTask;

    protected DataFeedTask contactDataFeedTask;

    protected DataFeedTask transactionDataFeedTask;

    protected DataFeedTask webVisitDataFeedTask;

    protected DataFeedTask webVisitPathPatternDataFeedTask;


    protected void prepareBaseData(EntityType entityType) throws Exception {
        switch (entityType) {
            case Accounts:
                baseAccountFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Accounts,
                        ACCOUNT_SOURCE_FILE);
                Assert.assertNotNull(baseAccountFile);
                startCDLImport(baseAccountFile, EntityType.Accounts);
                break;
            case Contacts:
                baseContactFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.Contacts,
                        CONTACT_SOURCE_FILE);
                Assert.assertNotNull(baseContactFile);
                startCDLImport(baseContactFile, EntityType.Contacts);
                break;
            case ProductPurchases:
                baseTransactionFile = uploadSourceFile(DEFAULT_SYSTEM, DEFAULT_SYSTEM_TYPE, EntityType.ProductPurchases,
                        TRANSACTION_SOURCE_FILE);
                Assert.assertNotNull(baseTransactionFile);
                startCDLImport(baseTransactionFile, EntityType.ProductPurchases);
                break;
            default:
        }
    }

    protected void getDataFeedTask(EntityType entityType) {
        DataFeedTask task = dataFeedProxy.getDataFeedTask(customerSpace, SOURCE,
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entityType),
                entityType.getEntity().name());
        switch (entityType) {
            case Accounts:
                accountDataFeedTask = task;
                break;
            case Contacts:
                contactDataFeedTask = task;
                break;
            case ProductPurchases:
                transactionDataFeedTask = task;
                break;
            case WebVisit:
                webVisitDataFeedTask = task;
                break;
            case WebVisitPathPattern:
                webVisitPathPatternDataFeedTask = task;
                break;
            default:
        }
    }

    protected SourceFile uploadSourceFile(String systemName, String systemType, EntityType entityType,
                                          String csvFileName) throws Exception {
        SourceFile sourceFile = fileUploadService.uploadFile("file_" + DateTime.now().getMillis() + ".csv",
                entityType.getSchemaInterpretation(), entityType.getEntity().name(), csvFileName,
                ClassLoader.getSystemResourceAsStream(SOURCE_FILE_LOCAL_PATH + csvFileName));

        S3ImportSystem s3ImportSystem = cdlService.getS3ImportSystem(mainTestTenant.getId(), systemName);
        if(s3ImportSystem == null) {
            cdlService.createS3ImportSystem(mainTestTenant.getId(), systemName, S3ImportSystem.SystemType.valueOf(systemType),
                    false);
        }
        String fileName = sourceFile.getName();
        FetchFieldDefinitionsResponse fetchFieldDefinitionsResponse =
                dataMappingService.fetchFieldDefinitions(systemName, systemType, entityType.getDisplayName(),
                        fileName);

        FieldDefinitionsRecord fieldDefinitionRecord = fetchFieldDefinitionsResponse.getCurrentFieldDefinitionsRecord();
        dataMappingService.commitFieldDefinitions(systemName, systemType,
                entityType.getDisplayName(), fileName, false, fieldDefinitionRecord);
        sourceFile = sourceFileService.findByName(sourceFile.getName());

        return sourceFile;
    }

    protected void startCDLImport(SourceFile sourceFile, EntityType entityType) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entityType.getEntity().name(),
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM,
                        entityType));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
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

    protected void verifyFailed(SourceFile sourceFile, EntityType entityType) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entityType.getEntity().name(),
                EntityTypeUtils.generateFullFeedType(DEFAULT_SYSTEM, entityType));

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
}
