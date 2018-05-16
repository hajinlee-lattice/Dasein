package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.cdl.PredictionType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineStatus;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CrossSellModelingConfig;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AggregationFilter;
import com.latticeengines.domain.exposed.query.AggregationSelector;
import com.latticeengines.domain.exposed.query.AggregationType;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.TransactionRestriction;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.serviceapps.cdl.ReportConstants;
import com.latticeengines.domain.exposed.util.ActivityMetricsUtils;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.proxy.exposed.cdl.ActionProxy;
import com.latticeengines.proxy.exposed.cdl.ActivityMetricsProxy;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.cdl.ServingStoreProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.testframework.exposed.proxy.pls.ModelingFileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public abstract class DataIngestionEnd2EndDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final Logger logger = LoggerFactory.getLogger(DataIngestionEnd2EndDeploymentTestNGBase.class);

    private static final String INITIATOR = "test@lattice-engines.com";
    private static final String S3_VDB_DIR = "le-serviceapps/cdl/end2end/vdb";
    private static final String S3_VDB_VERSION = "2";

    private static final String S3_CSV_DIR = "le-serviceapps/cdl/end2end/csv";
    private static final String S3_CSV_VERSION = "2";

    private static final String SEGMENT_NAME_1 = NamingUtils.timestamp("E2ESegment1");
    static final long SEGMENT_1_ACCOUNT_1 = 21;
    static final long SEGMENT_1_CONTACT_1 = 23;
    static final long SEGMENT_1_ACCOUNT_2 = 30;
    static final long SEGMENT_1_CONTACT_2 = 32;
    static final long SEGMENT_1_ACCOUNT_3 = 54;
    static final long SEGMENT_1_CONTACT_3 = 63;
    static final long SEGMENT_1_ACCOUNT_4 = 58;
    static final long SEGMENT_1_CONTACT_4 = 68;

    private static final String SEGMENT_NAME_2 = NamingUtils.timestamp("E2ESegment2");
    static final long SEGMENT_2_ACCOUNT_1 = 12;
    static final long SEGMENT_2_CONTACT_1 = 12;
    static final long SEGMENT_2_ACCOUNT_2 = 45;
    static final long SEGMENT_2_CONTACT_2 = 49;
    static final long SEGMENT_2_ACCOUNT_2_REBUILD = 44;
    static final long SEGMENT_2_CONTACT_2_REBUILD = 49;

    static final String SEGMENT_NAME_MODELING = NamingUtils.timestamp("E2ESegmentModeling");
    static final String SEGMENT_NAME_TRAINING = NamingUtils.timestamp("E2ESegmentTraining");

    static final long RATING_A_COUNT_1 = 6;
    static final long RATING_D_COUNT_1 = 5;
    static final long RATING_F_COUNT_1 = 1;

    static final long RATING_A_COUNT_2 = 20;
    static final long RATING_D_COUNT_2 = 22;
    static final long RATING_F_COUNT_2 = 0;

    static final long RATING_A_COUNT_2_REBUILD = 18;
    static final long RATING_D_COUNT_2_REBUILD = 31;
    static final long RATING_F_COUNT_2_REBUILD = 4;

    static final String SEGMENT_PRODUCT_ID = "A80D4770376C1226C47617C071324C0B";
    static final String TARGET_PRODUCT = "B0829F745A42D18FE77050EC05A51D2F";
    static final String TRAINING_PRODUCT = "6368494B622E0CB60F9C80FEB1D0F95F";

    static final int EARLIEST_TRANSACTION = 48033;
    static final int LATEST_TRANSACTION = 48929;

    int actionsNumber = 0;

    @Inject
    DataCollectionProxy dataCollectionProxy;

    @Inject
    DataFeedProxy dataFeedProxy;

    @Inject
    CDLProxy cdlProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected ModelingFileUploadProxy fileUploadProxy;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    private CheckpointService checkpointService;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    private TestMetadataSegmentProxy testMetadataSegmentProxy;

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Inject
    private ServingStoreProxy servingStoreProxy;

    @Inject
    protected PeriodProxy periodProxy;

    @Inject
    private ActionProxy actionProxy;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    protected String processAnalyzeAppId;
    protected DataCollection.Version initialVersion;

    protected RatingEngine ratingEngine;

    @BeforeClass(groups = { "end2end", "precheckin", "deployment", "end2end_with_import" })
    public void setup() throws Exception {
        setupEnd2EndTestEnvironment();
    }

    @AfterClass(groups = { "end2end", "precheckin" })
    protected void cleanup() throws Exception {
        checkpointService.cleanup();
    }

    protected void setupEnd2EndTestEnvironment() throws Exception {
        logger.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        checkpointService.setMainTestTenant(mainTestTenant);

        logger.info("Test environment setup finished.");
        createDataFeed();
        updateDataCloudBuildNumber();
        setupBusinessCalendar();
        setupPurchaseHistoryMetrics();

        attachProtectedProxy(fileUploadProxy);
        attachProtectedProxy(testMetadataSegmentProxy);
    }

    protected void resetCollection() {
        logger.info("Start reset collection data ...");
        boolean resetStatus = cdlProxy.reset(mainTestTenant.getId());
        assertEquals(resetStatus, true);
    }

    void processAnalyze() {
        processAnalyze(null);
    }

    void processAnalyze(ProcessAnalyzeRequest request) {
        logger.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), request);
        processAnalyzeAppId = appId.toString();
        logger.info("processAnalyzeAppId=" + processAnalyzeAppId);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
    }

    void uploadAccountCSV() {
        Resource csvResrouce = new ClassPathResource("end2end/csv/Account1.csv",
                Thread.currentThread().getContextClassLoader());
        SourceFile sourceFile = fileUploadProxy.uploadFile("Account1.csv", false, "Account1.csv",
                SchemaInterpretation.Account, "Account", csvResrouce);
        logger.info("Uploaded file " + sourceFile.getName() + " to " + sourceFile.getPath());
    }

    SourceFile uploadDeleteCSV(String fileName, SchemaInterpretation schema, CleanupOperationType type,
            Resource source) {
        logger.info("Upload file " + fileName + ", operation type is " + type.name() + ", Schema is " + schema.name());
        return fileUploadProxy.uploadDeleteFile(false, fileName, schema.name(), type.name(), source);
    }

    void mockVdbImport(BusinessEntity entity, int offset, int limit) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), SourceType.VISIDB.getName(), "Query",
                entity.name());
        Table importTemplate;
        if (dataFeedTask == null) {
            Schema schema = getVdbImportSchema(entity);
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            switch (entity) {
            case Account:
                importTemplate.getAttributes().forEach(attr -> {
                    attr.setGroupsViaList(Arrays.asList( //
                            ColumnSelection.Predefined.TalkingPoint, //
                            ColumnSelection.Predefined.CompanyProfile));
                    if (attr.getName().equals("Id") || attr.getName().equals("AccountId")) {
                        attr.setInterfaceName(InterfaceName.AccountId);
                    }
                });
                importTemplate.setName(SchemaInterpretation.Account.name());
                break;
            case Contact:
                importTemplate.setName(SchemaInterpretation.Contact.name());
                importTemplate.getAttributes().forEach(attr -> {
                    if (attr.getName().equals("Id") || attr.getName().equals("ContactId")) {
                        attr.setInterfaceName(InterfaceName.ContactId);
                    }
                });
                break;
            case Product:
                importTemplate.setName(SchemaInterpretation.Product.name());
                break;
            default:
                importTemplate.setName(SchemaInterpretation.Transaction.name());
            }
            dataFeedTask = new DataFeedTask();
            dataFeedTask.setImportTemplate(importTemplate);
            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity.name());
            dataFeedTask.setFeedType("Query");
            dataFeedTask.setSource("VisiDB");
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
        } else {
            importTemplate = dataFeedTask.getImportTemplate();
        }

        String targetPath = uploadMockDataWithModifiedSchema(entity, offset, limit);
        String defaultFS = yarnConfiguration.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        Extract e = createExtract(hdfsUri, (long) limit);
        dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query", entity.name());
        List<String> tables = dataFeedProxy.registerExtract(customerSpace.toString(), dataFeedTask.getUniqueId(),
                importTemplate.getName(), e);
        registerImportAction(dataFeedTask);
        dataFeedProxy.addTablesToQueue(customerSpace.toString(), dataFeedTask.getUniqueId(), tables);
    }

    void importData(BusinessEntity entity, int offset, int limit) {
        String templateName = String.format("%s_%d_%d.csv", entity.name(), offset, limit);
        importData(entity, templateName);
    }

    void importData(BusinessEntity entity, String s3FileName) {
        Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(s3FileName), s3FileName);
        logger.info("Streaming S3 file " + s3FileName + " as a template file for " + entity);
        SourceFile template = fileUploadProxy.uploadFile(s3FileName, false, s3FileName, entity.name(), csvResource);
        FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(), entity.name());
        if (BusinessEntity.Account.equals(entity)) {
            setExternalSystem(fieldMappingDocument.getFieldMappings());
        }
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
        fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument, entity.name(),
                SourceType.FILE.getName(),entity.name() + "Schema");
        logger.info("Modified field mapping document is saved, start importing ...");
        ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), entity.name() + "Schema",
                template, template, INITIATOR);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        logger.info("Importing S3 file " + s3FileName + " for " + entity + " is finished.");
    }

    private void setExternalSystem(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getUserField().equalsIgnoreCase("SalesforceAccountID")) {
                fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.CRM);
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equalsIgnoreCase("SalesforceSandboxAccountID")) {
                fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.CRM);
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
            if (fieldMapping.getUserField().equalsIgnoreCase("MarketoAccountID")) {
                fieldMapping.setCdlExternalSystemType(CDLExternalSystemType.ERP);
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }
    }

    private ApplicationId submitImport(String customerSpace, String entity, String feedType,
            SourceFile templateSourceFile, SourceFile dataSourceFile, String email) {
        String source = SourceType.FILE.getName();
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateSourceFile, dataSourceFile, email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace, SourceType.FILE.getName(), entity, feedType, metaData);
        logger.info("Creating a data feed task for " + entity + " with id " + taskId);
        if (StringUtils.isEmpty(taskId)) {
            throw new LedpException(LedpCode.LEDP_18162, new String[] { entity, source, feedType });
        }
        return cdlProxy.submitImportJob(customerSpace, taskId, metaData);
    }

    private CSVImportConfig generateImportConfig(String customerSpace, SourceFile templateSourceFile,
            SourceFile dataSourceFile, String email) {
        CSVToHdfsConfiguration importConfig = new CSVToHdfsConfiguration();
        templateSourceFile.setTableName("SourceFile_" + templateSourceFile.getName().replace(".", "_"));
        importConfig.setCustomerSpace(CustomerSpace.parse(customerSpace));
        importConfig.setTemplateName(templateSourceFile.getTableName());
        importConfig.setFilePath(dataSourceFile.getPath());
        importConfig.setFileSource("HDFS");
        CSVImportFileInfo importFileInfo = new CSVImportFileInfo();
        importFileInfo.setFileUploadInitiator(email);
        importFileInfo.setReportFileDisplayName(dataSourceFile.getDisplayName());
        importFileInfo.setReportFileName(dataSourceFile.getName());
        CSVImportConfig csvImportConfig = new CSVImportConfig();
        csvImportConfig.setCsvToHdfsConfiguration(importConfig);
        csvImportConfig.setCSVImportFileInfo(importFileInfo);

        return csvImportConfig;
    }

    private InputStream readCSVInputStreamFromS3(String fileName) {
        return testArtifactService.readTestArtifactAsStream(S3_CSV_DIR, S3_CSV_VERSION, fileName);
    }

    private class MultipartFileResource extends InputStreamResource {

        private String fileName;

        public MultipartFileResource(InputStream inputStream, String fileName) {
            super(inputStream);
            this.fileName = fileName;
        }

        @Override
        public String getFilename() {
            return fileName;
        }

        @Override
        public long contentLength() {
            return -1;
        }
    }

    private void registerImportAction(DataFeedTask dataFeedTask) {
        logger.info(String.format("Registering action for dataFeedTask=%s", dataFeedTask));
        Action action = new Action();
        action.setType(ActionType.METADATA_CHANGE);
        action.setActionInitiator(INITIATOR);
        action.setDescription(dataFeedTask.getUniqueId());
        action.setTrackingId(null);
        actionProxy.createAction(mainCustomerSpace, action);
    }

    private String uploadMockDataWithModifiedSchema(BusinessEntity entity, int offset, int limit) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-" + entity + "/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()));
        String entityName = entity.name();
        List<GenericRecord> records;
        Schema schema;
        try {
            schema = AvroUtils.readSchemaFromInputStream(readInputStreamFromS3(entityName));
            records = AvroUtils.readFromInputStream(readInputStreamFromS3(entityName), offset, limit);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read generic records from input stream for entity " + entityName);
        }
        try {
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetPath + "/part-00000.avro", records, true);
            logger.info("Uploaded " + records.size() + " records to " + targetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro for " + entityName);
        }
        return targetPath;
    }

    private InputStream readInputStreamFromS3(String entityName) {
        return testArtifactService.readTestArtifactAsStream(S3_VDB_DIR, S3_VDB_VERSION, entityName + ".avro");
    }

    long importCsv(BusinessEntity entity, int fileId) throws Exception {
        String csvPath = String.format("end2end/csv/%s%d.csv", entity.name(), fileId);
        Resource csvResource = new ClassPathResource(csvPath, Thread.currentThread().getContextClassLoader());
        String templateName = String.format("%s%d_template.csv", entity, fileId);
        String dataName = String.format("%s%d_data.csv", entity, fileId);
        SourceFile template = fileUploadProxy.uploadFile(templateName, false, templateName, entity.name(), csvResource);
        SourceFile data = fileUploadProxy.uploadFile(dataName, false, dataName, entity.name(), csvResource);
        FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(), entity.name());
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(true);
            }
        }
        fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument);
        long startTime = System.currentTimeMillis();
        ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), "e2etest", template,
                data, INITIATOR);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(
                applicationId.toString(), false);
        long endTime = System.currentTimeMillis();
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
        verifyActionRegistration();
        return tryGetAvroFileRows(startTime, endTime);
    }

    protected void verifyActionRegistration() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<Action> actions = actionProxy.getActionsByOwnerId(customerSpace.toString(), null);
        // Assert.assertEquals(actions.size(), ++actionsNumber);
    }

    private long tryGetAvroFileRows(long startMillis, long endMillis) throws Exception {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts",
                PathBuilder
                        .buildDataTablePath(CamilleEnvironment.getPodId(), CustomerSpace.parse(mainTestTenant.getId()))
                        .toString(),
                SourceType.FILE.getName());
        Assert.assertTrue(HdfsUtils.fileExists(yarnConfiguration, targetPath));
        List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, targetPath);
        for (String file : files) {
            String filename = file.substring(file.lastIndexOf("/") + 1);
            Date folderTime = new SimpleDateFormat(COLLECTION_DATE_FORMAT).parse(filename);
            if (folderTime.getTime() > startMillis && folderTime.getTime() < endMillis) {
                logger.info("Find matched file: " + filename);
                HdfsUtils.HdfsFileFilter filter = file1 -> {
                    if (file1 == null) {
                        return false;
                    }

                    String name = file1.getPath().getName();
                    return name.endsWith(".avro");
                };
                List<String> avroFiles = HdfsUtils.getFilesForDirRecursive(yarnConfiguration, file, filter);
                Assert.assertTrue(avroFiles.size() > 0);
                String avroFilePath = avroFiles.get(0).substring(0, avroFiles.get(0).lastIndexOf("/"));

                return AvroUtils.count(yarnConfiguration, avroFilePath + "/*.avro");
            }
        }
        Assert.fail("No data collection folder was created!");
        return 0L;
    }

    private Schema getVdbImportSchema(BusinessEntity entity) {
        InputStream avroIs = readInputStreamFromS3(entity.name());
        try {
            return AvroUtils.readSchemaFromInputStream(avroIs);
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare avro schema for " + entity);
        }
    }

    private Extract createExtract(String path, long processedRecords) {
        Extract e = new Extract();
        e.setName(StringUtils.substringAfterLast(path, "/"));
        e.setPath(PathUtils.stripoutProtocol(path));
        e.setProcessedRecords(processedRecords);
        String dateTime = StringUtils.substringBetween(path, "/Extracts/", "/");
        SimpleDateFormat f = new SimpleDateFormat(COLLECTION_DATE_FORMAT);
        try {
            e.setExtractionTimestamp(f.parse(dateTime).getTime());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return e;
    }

    private void createDataFeed() {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Table importTable = new Table();
        importTable.setName("importTable");
        importTable.setDisplayName(importTable.getName());
        importTable.setTenant(mainTestTenant);
        Table dataTable = new Table();
        dataTable.setName("dataTable");
        dataTable.setDisplayName(dataTable.getName());
        dataTable.setTenant(mainTestTenant);

    }

    protected void updateDataCloudBuildNumber() {
        String currentDataCloudBuildNumber = columnMetadataProxy.latestVersion(null).getDataCloudBuildNumber();
        dataCollectionProxy.updateDataCloudBuildNumber(mainTestTenant.getId(), currentDataCloudBuildNumber);
    }

    long countTableRole(TableRoleInCollection role) {
        return checkpointService.countTableRole(role);
    }

    long countInRedshift(BusinessEntity entity) {
        return checkpointService.countInRedshift(entity);
    }

    String getTableName(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        return dataCollectionProxy.getTableName(customerSpace.toString(), role);
    }

    void resumeCheckpoint(String checkpoint) throws IOException {
        checkpointService.resumeCheckpoint(checkpoint);
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
    }

    void saveCheckpoint(String checkpoint) throws IOException {
        checkpointService.saveCheckPoint(checkpoint);
    }

    private List<Report> retrieveReport(String appId) {
        Job job = testBed.getRestTemplate().getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", deployedHostPort, appId), //
                Job.class);
        assertNotNull(job);
        return job.getReports();
    }

    void verifyStats(BusinessEntity... entities) {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubeMap = container.getStatsCubes();
        for (BusinessEntity entity : entities) {
            Assert.assertTrue(cubeMap.containsKey(entity.name()), "Stats should contain a cube for " + entity);
        }
    }

    void verifyProcessAnalyzeReport(String appId) {
        List<Report> reports = retrieveReport(appId);
        assertEquals(reports.size(), 1);
        Report summaryReport = reports.get(0);
        verifyConsolidateSummaryReport(summaryReport);
    }

    void verifyConsolidateSummaryReport(Report summaryReport) {

        Assert.assertNotNull(summaryReport);
        Assert.assertNotNull(summaryReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(summaryReport.getJson().getPayload()));

        logger.info("ConsolidateSummaryReport: " + summaryReport.getJson().getPayload());

        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(summaryReport.getJson().getPayload());
            ObjectNode entitiesSummaryNode = (ObjectNode) report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());

            BusinessEntity[] entities = { BusinessEntity.Account, BusinessEntity.Contact, BusinessEntity.Product,
                    BusinessEntity.Transaction };
            for (BusinessEntity entity : entities) {
                Assert.assertTrue(entitiesSummaryNode.has(entity.name()));
                ObjectNode entityNode = (ObjectNode) entitiesSummaryNode.get(entity.name());
                Assert.assertNotNull(entityNode);

                ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
                Assert.assertNotNull(consolidateSummaryNode);

                if (entity != BusinessEntity.Product) {
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.NEW));
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.DELETE));
                } else {
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.PRODUCT_ID));
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.PRODUCT_BUNDLE));
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.PRODUCT_HIERARCHY));
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.ERROR_MESSAGE));
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.WARN_MESSAGE));
                }

                if (entity == BusinessEntity.Account || entity == BusinessEntity.Contact) {
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.UPDATE));
                }
                if (entity == BusinessEntity.Account) {
                    Assert.assertTrue(consolidateSummaryNode.has(ReportConstants.UNMATCH));
                }

                if (entity != BusinessEntity.Product) {
                    ObjectNode entityNumberNode = (ObjectNode) entityNode
                            .get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey());
                    Assert.assertNotNull(entityNumberNode);
                    Assert.assertTrue(entityNumberNode.has(ReportConstants.TOTAL));
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + summaryReport.getJson().getPayload(), e);
        }
    }

    void verifyDataFeedStatus(DataFeed.Status expected) {
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertNotNull(dataFeed);
        Assert.assertEquals(dataFeed.getStatus(), expected);
    }

    void verifyActiveVersion(DataCollection.Version expected) {
        Assert.assertEquals(dataCollectionProxy.getActiveVersion(mainTestTenant.getId()), expected);
    }

    void createTestSegments() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment1());
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment2());
        MetadataSegment segment1 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_1);
        MetadataSegment segment2 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_2);
        Assert.assertNotNull(segment1);
        Assert.assertNotNull(segment2);
    }

    void createTestSegment1() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment1());
        MetadataSegment segment1 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_1);
        Assert.assertNotNull(segment1);
    }

    void createTestSegment2() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment2());
        MetadataSegment segment2 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_2);
        Assert.assertNotNull(segment2);
    }

    void createModelingSegment() {
        testMetadataSegmentProxy.createOrUpdate(constructTargetSegment());
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_MODELING);
        Assert.assertNotNull(segment);
    }

    private MetadataSegment constructTestSegment1() {
        Bucket websiteBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList(".com"));
        BucketRestriction websiteRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), websiteBkt);
        Bucket.Transaction txn = new Bucket.Transaction("A80D4770376C1226C47617C071324C0B", TimeFilter.ever(), null,
                null, false);
        Bucket purchaseBkt = Bucket.txnBkt(txn);
        BucketRestriction purchaseRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Transaction, "AnyName"), purchaseBkt);
        Restriction accountRestriction = Restriction.builder().and(websiteRestriction, purchaseRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("Buyer"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_1);
        segment.setDisplayName("End2End Segment 1");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    protected MetadataSegment constructTestSegment2() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.IN_COLLECTION,
                Arrays.asList("CALIFORNIA", "TEXAS", "MICHIGAN", "NEW YORK"));
        BucketRestriction stateRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "LDC_State"), stateBkt);
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("Moderate"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "BmbrSurge_EmployeeScreening_Intent"), techBkt);
        Restriction accountRestriction = Restriction.builder().or(stateRestriction, techRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_2);
        segment.setDisplayName("End2End Segment 2");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    MetadataSegment constructTargetSegment() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Arrays.asList("VT"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "State"), stateBkt);
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_MODELING);
        segment.setDisplayName("End2End Segment Modeling");
        segment.setDescription("A test segment for CDL end2end modeling test.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setAccountRestriction(accountRestriction);
        return segment;
    }

    MetadataSegment constructTrainingSegment() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("No"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "OUT_OF_BUSINESS_INDICATOR"), stateBkt);
        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_TRAINING);
        segment.setDisplayName("End2End Segment Training");
        segment.setDescription("A training segment for CDL end2end modeling test.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setAccountRestriction(accountRestriction);
        return segment;
    }

    void verifyTestSegment1Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_1, expectedCounts);
    }

    void verifyTestSegment2Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_2, expectedCounts);
    }

    private void verifySegmentCounts(String segmentName, Map<BusinessEntity, Long> expectedCounts) {
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(segmentName);
        int retries = 0;
        while (segment == null && retries++ < 3) {
            logger.info("Wait for 1 sec to retry getting rating engine.");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }
            segment = testMetadataSegmentProxy.getSegment(segmentName);
        }
        Assert.assertNotNull(segment,
                "Cannot find rating engine " + segmentName + " in tenant " + mainTestTenant.getId());
        final MetadataSegment immutableSegment = segment;
        expectedCounts.forEach((entity, count) -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            Assert.assertEquals(immutableSegment.getEntityCount(entity), count, JsonUtils.pprint(immutableSegment));
        });
    }

    RatingEngine createRuleBasedRatingEngine() {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setSegment(constructTestSegment2());
        ratingEngine.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setType(RatingEngineType.RULE_BASED);
        RatingEngine newEngine = ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);

        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        newEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());

        Assert.assertNotNull(newEngine);
        Assert.assertNotNull(newEngine.getActiveModel(), JsonUtils.pprint(newEngine));

        String modelId = newEngine.getActiveModel().getId();
        RuleBasedModel model = constructRuleModel(modelId);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), modelId, model);

        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    void activateRatingEngine(String engineId) {
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        if (ratingEngine == null) {
            throw new IllegalArgumentException("Cannot find the engine to be activated " + engineId);
        }
        ratingEngine.setStatus(RatingEngineStatus.ACTIVE);
        ratingEngineProxy.createOrUpdateRatingEngine(mainTestTenant.getId(), ratingEngine);
    }

    private RuleBasedModel constructRuleModel(String modelId) {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt("CALIFORNIA");
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_State"), bktA);
        ratingRule.setRuleForBucket(RatingBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("BOB"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RatingBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setId(modelId);
        return ruleBasedModel;
    }

    MetadataSegment targetSegmentWithTrxRestrictions() {
        // this filters out this account in the test data 0012400001DO2fqAAD
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Arrays.asList("VT"));
        BucketRestriction accountRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "State"), stateBkt);

        TransactionRestriction invalidTrxRes = new TransactionRestriction(SEGMENT_PRODUCT_ID,
                new TimeFilter(ComparisonType.BEFORE, PeriodStrategy.Template.Date.name(),
                        Collections.singletonList("2018-04-09")),
                false,
                new AggregationFilter(AggregationSelector.SPENT, AggregationType.SUM, ComparisonType.GREATER_THAN,
                        Collections.singletonList(1)),
                new AggregationFilter(AggregationSelector.UNIT, AggregationType.SUM, ComparisonType.GREATER_THAN,
                        Collections.singletonList(1)));

        TransactionRestriction validTrxRes = new TransactionRestriction(SEGMENT_PRODUCT_ID,
                new TimeFilter(ComparisonType.BETWEEN, PeriodStrategy.Template.Month.name(), Arrays.asList(3, 6)),
                false, null, null);

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_MODELING);
        segment.setDisplayName("End2End Segment Modeling");
        segment.setDescription("A test segment for CDL end2end modeling test.");
        segment.setAccountRestriction(Restriction.builder() //
                .and(accountRestriction, invalidTrxRes, validTrxRes).build());
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(segment.getAccountRestriction()));

        return segment;
    }

    RatingEngine constructRatingEngine(RatingEngineType engineType, MetadataSegment targetSegment) {
        RatingEngine ratingEngine = new RatingEngine();
        ratingEngine.setDisplayName("CDL End2End " + engineType + " Engine");
        ratingEngine.setTenant(mainTestTenant);
        ratingEngine.setType(engineType);
        ratingEngine.setSegment(targetSegment);
        ratingEngine.setCreatedBy(TestFrameworkUtils.SUPER_ADMIN_USERNAME);
        ratingEngine.setCreated(new Date());
        return ratingEngine;
    }

    void configureCrossSellModel(AIModel testAIModel, PredictionType predictionType, String targetProductId,
            String trainingProductId) {
        testAIModel.setPredictionType(predictionType);

        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(testAIModel);
        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> myMap = new HashMap<>();
        myMap.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, new ModelingConfigFilter(
                CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, ComparisonType.PRIOR_ONLY, 6));
        config.setFilters(myMap);

        config.setModelingStrategy(ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE);
        config.setTargetProducts(Collections.singletonList(targetProductId));
        config.setTrainingProducts(Collections.singletonList(trainingProductId));
    }

    void configureCustomEventModel(AIModel testAIModel) {
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(testAIModel);
        advancedConf.setDataStores(
                Arrays.asList(CustomEventModelingConfig.DataStore.CDL, CustomEventModelingConfig.DataStore.DataCloud));
        advancedConf.setCustomEventModelingType(CustomEventModelingType.CDL);
        advancedConf.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        advancedConf.setExcludePublicDomains(false);
    }

    void verifyRatingEngineCount(String engineId, Map<RatingBucketName, Long> expectedCounts) {
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        int retries = 0;
        while (ratingEngine == null && retries++ < 3) {
            logger.info("Wait for 1 sec to retry getting rating engine.");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }
            ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        }
        Assert.assertNotNull(ratingEngine,
                "Cannot find rating engine " + engineId + " in tenant " + mainTestTenant.getId());
        System.out.println(JsonUtils.pprint(ratingEngine));
        Map<String, Long> counts = ratingEngine.getCountsAsMap();
        Assert.assertTrue(MapUtils.isNotEmpty(counts));
        expectedCounts.forEach((bkt, count) -> {
            if (count > 0) {
                Assert.assertNotNull(counts.get(bkt.getName()),
                        "Cannot find count for bucket " + bkt.getName() + " in rating engine.");
                Assert.assertEquals(counts.get(bkt.getName()), count, "Rating engine count " + bkt.getName()
                        + " expected " + counts.get(bkt.getName()) + " found " + count);
            }
        });
    }

    List<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity) {
        return servingStoreProxy.getDecoratedMetadataFromCache(mainCustomerSpace, entity);
    }

    void verifyUpdateActions() {
        List<Action> actions = actionProxy.getActions(mainTestTenant.getId());
        logger.info(String.format("actions=%s", actions));
        Assert.assertTrue(CollectionUtils.isNotEmpty(actions));
        Assert.assertTrue(actions.stream().allMatch(action -> action.getOwnerId() != null));
    }

    void runCommonPAVerifications() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion.complement());
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer, "Should have statistics in active version");
    }

    private BusinessCalendar createBusinessCalendar() {
        BusinessCalendar calendar = new BusinessCalendar();
        calendar.setMode(BusinessCalendar.Mode.STARTING_DATE);
        calendar.setStartingDate("JAN-01");
        calendar.setLongerMonth(1);
        calendar.setCreated(new Date());
        calendar.setUpdated(new Date());
        return calendar;
    }

    void setupBusinessCalendar() {
        periodProxy.saveBusinessCalendar(mainTestTenant.getId(), createBusinessCalendar());
    }

    void setupPurchaseHistoryMetrics() {
        List<ActivityMetrics> metrics = ActivityMetricsUtils.fakePurchaseMetrics(mainTestTenant);
        activityMetricsProxy.save(mainCustomerSpace, ActivityType.PurchaseHistory, metrics);
    }

}
