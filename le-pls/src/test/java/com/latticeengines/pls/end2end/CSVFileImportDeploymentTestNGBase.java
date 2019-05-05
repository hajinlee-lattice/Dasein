package com.latticeengines.pls.end2end;

import static org.testng.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;

import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.query.EntityType;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.functionalframework.CDLDeploymentTestNGBase;
import com.latticeengines.pls.service.CDLService;
import com.latticeengines.pls.service.FileUploadService;
import com.latticeengines.pls.service.ModelingFileMetadataService;
import com.latticeengines.pls.service.SourceFileService;
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

    protected static final String ACCOUNT_SOURCE_FILE = "Account_base.csv";
    protected static final String CONTACT_SOURCE_FILE = "Contact_base.csv";
    protected static final String TRANSACTION_SOURCE_FILE = "Transaction_base.csv";

    protected static final String ACCOUNT_SOURCE_FILE_FROMATDATE = "Account_FormatDate.csv";

    protected static final String ACCOUNT_SOURCE_FILE_MISSING = "Account_missing_Website.csv";
    protected static final String TRANSACTION_SOURCE_FILE_MISSING = "Transaction_missing_required.csv";

    @Autowired
    protected FileUploadService fileUploadService;

    @Autowired
    protected SourceFileService sourceFileService;

    @Autowired
    protected ModelingFileMetadataService modelingFileMetadataService;

    @Autowired
    protected WorkflowProxy workflowProxy;

    @Autowired
    protected DataFeedProxy dataFeedProxy;

    @Autowired
    protected CDLService cdlService;

    @Autowired
    protected Configuration yarnConfiguration;

    protected SourceFile baseAccountFile;

    protected SourceFile baseContactFile;

    protected SourceFile baseTransactionFile;

    protected DataFeedTask accountDataFeedTask;

    protected DataFeedTask contactDataFeedTask;

    protected DataFeedTask transactionDataFeedTask;


    protected void prepareBaseData(String entity) {
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

    protected void startCDLImport(SourceFile sourceFile, String entity) {
        ApplicationId applicationId = cdlService.submitCSVImport(customerSpace, sourceFile.getName(),
                sourceFile.getName(), SOURCE, entity, getFeedTypeByEntity(DEFAULT_SYSTEM, entity));

        JobStatus completedStatus = waitForWorkflowStatus(workflowProxy, applicationId.toString(), false);
        assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    public String getFeedTypeByEntity(String system, String entity) {
        String feedType = entity + FEED_TYPE_SUFFIX;
        switch (entity) {
            case ENTITY_ACCOUNT: feedType =
                    system + SPLIT_CHART + EntityType.Accounts.getDefaultFeedTypeName();break;
            case ENTITY_CONTACT: feedType =
                    system + SPLIT_CHART + EntityType.Contacts.getDefaultFeedTypeName();break;
            case ENTITY_TRANSACTION: feedType =
                    system + SPLIT_CHART + EntityType.ProductPurchases.getDefaultFeedTypeName();break;
            default:break;
        }
        return feedType;
    }
}
