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
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.core.type.TypeReference;
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
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ProcessAnalyzeRequest;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Category;
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
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.pls.RuleBasedModel;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.pls.frontend.FieldMapping;
import com.latticeengines.domain.exposed.pls.frontend.FieldMappingDocument;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.domain.exposed.workflow.ReportPurpose;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.testframework.exposed.proxy.pls.ModelingFileUploadProxy;
import com.latticeengines.testframework.exposed.proxy.pls.PlsCDLImportProxy;
import com.latticeengines.testframework.exposed.proxy.pls.TestMetadataSegmentProxy;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;

public abstract class DataIngestionEnd2EndDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final Logger logger = LoggerFactory.getLogger(DataIngestionEnd2EndDeploymentTestNGBase.class);

    private static final String INITIATOR = "test@lattice-engines.com";
    private static final String S3_VDB_DIR = "le-serviceapps/cdl/end2end/vdb";
    private static final String S3_VDB_VERSION = "1";

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
    static final long SEGMENT_2_ACCOUNT_1 = 24;
    static final long SEGMENT_2_CONTACT_1 = 27;
    static final long SEGMENT_2_ACCOUNT_2 = 67;
    static final long SEGMENT_2_CONTACT_2 = 81;

    static final long RATING_A_COUNT_1 = 6;
    static final long RATING_D_COUNT_1 = 17;
    static final long RATING_F_COUNT_1 = 1;

    static final long RATING_A_COUNT_2 = 20;
    static final long RATING_D_COUNT_2 = 45;
    static final long RATING_F_COUNT_2 = 2;

    int actionsNumber = 0;

    @Inject
    DataCollectionProxy dataCollectionProxy;

    @Inject
    DataFeedProxy dataFeedProxy;

    @Inject
    CDLProxy cdlProxy;

    @Inject
    private PlsCDLImportProxy plsCDLImportProxy;

    @Inject
    private ModelingFileUploadProxy fileUploadProxy;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private JobService jobService;

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

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    protected String consolidateAppId;
    protected String profileAppId;
    protected String processAnalyzeAppId;
    protected DataCollection.Version initialVersion;

    protected RatingEngine ratingEngine;;

    @BeforeClass(groups = { "end2end", "precheckin" })
    public void setup() throws Exception {
        logger.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        // testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        checkpointService.setMainTestTenant(mainTestTenant);

        logger.info("Test environment setup finished.");
        createDataFeed();

        attachProtectedProxy(fileUploadProxy);
        attachProtectedProxy(plsCDLImportProxy);
        attachProtectedProxy(testMetadataSegmentProxy);
    }

    @AfterClass(groups = { "end2end", "precheckin" })
    protected void cleanup() throws Exception {
        checkpointService.cleanup();
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

    String uploadDeleteCSV(String fileName, SchemaInterpretation schema, CleanupOperationType type, Resource source) {
        logger.info("Upload file " + fileName + ", operation type is " + type.name() + ", Schema is " + schema.name());
        return fileUploadProxy.uploadDeleteFile(false, fileName, schema.name(), type.name(), source);
    }

    void mockVdbImport(BusinessEntity entity, int offset, int limit) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query",
                entity.name());
        Table importTemplate;
        if (dataFeedTask == null) {
            Schema schema = getVdbImportSchema(entity);
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            switch (entity) {
            case Account:
                importTemplate.getAttributes()
                        .forEach(attr -> attr.setGroupsViaList(Arrays.asList( //
                                ColumnSelection.Predefined.TalkingPoint, //
                                ColumnSelection.Predefined.CompanyProfile)));
                importTemplate.setName(SchemaInterpretation.Account.name());
                break;
            case Contact:
                importTemplate.setName(SchemaInterpretation.Contact.name());
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
        dataFeedProxy.registerExtract(customerSpace.toString(), dataFeedTask.getUniqueId(), importTemplate.getName(),
                e);
        registerImportAction(dataFeedTask);
    }

    private Action registerImportAction(DataFeedTask dataFeedTask) {
        logger.info(String.format("Regsitering action for dataFeedTask=%s", dataFeedTask));
        Action action = new Action();
        action.setType(ActionType.METADATA_CHANGE);
        action.setActionInitiator(INITIATOR);
        action.setTenant(mainTestTenant);
        action.setDescription(dataFeedTask.getUniqueId());
        action.setTrackingId(null);

        return internalResourceProxy.createAction(mainTestTenant.getId(), action);
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
        ApplicationId applicationId = plsCDLImportProxy.startImportCSV(template.getName(), data.getName(), "File",
                entity.name(), "e2etest");
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(
                applicationId.toString(), false);
        long endTime = System.currentTimeMillis();
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
        verifyActionRegistration();
        return tryGetAvroFileRows(startTime, endTime);
    }

    protected void verifyActionRegistration() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<Action> actions = internalResourceProxy.findAll(customerSpace.toString());
        Assert.assertEquals(actions.size(), ++actionsNumber);
    }

    long importCsvForCleanup(BusinessEntity entity) throws Exception {
        String csvPath = String.format("end2end/csv/%s_base.csv", entity.name());
        Resource csvResource = new ClassPathResource(csvPath, Thread.currentThread().getContextClassLoader());
        String templateName = String.format("%s_template.csv", entity);
        String dataName = String.format("%s_data.csv", entity);
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
        ApplicationId applicationId = plsCDLImportProxy.startImportCSV(template.getName(), data.getName(), "File",
                entity.name(), "e2etest");
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(
                applicationId.toString(), false);
        long endTime = System.currentTimeMillis();
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
        return tryGetAvroFileRows(startTime, endTime);
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

    long countTableRole(TableRoleInCollection role) {
        return checkpointService.countTableRole(role);
    }

    long countInRedshift(BusinessEntity entity) {
        return checkpointService.countInRedshift(entity);
    }

    void verifyFirstProfileCheckpoint() throws IOException {
        checkpointService.verifyFirstProfileCheckpoint();
    }

    void verifySecondConsolidateCheckpoint() throws IOException {
        checkpointService.verifySecondConsolidateCheckpoint();
    }

    void verifySecondProfileCheckpoint() throws IOException {
        checkpointService.verifySecondProfileCheckpoint();
    }

    void resumeCheckpoint(String checkpoint) throws IOException {
        checkpointService.resumeCheckpoint(checkpoint);
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
    }

    void saveCheckpoint(String checkpoint) throws IOException {
        checkpointService.saveCheckPoint(checkpoint);
    }

    boolean exportEntityToRedshift(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), role);

        logger.info("Started exporting " + role + " to redshift ...");
        ExportConfiguration exportConfiguration = setupExportConfig(table, table.getName(), role);
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfiguration);
        int timeout = new Long(TimeUnit.MINUTES.toSeconds(60)).intValue();
        logger.info("Waiting for " + submission.getApplicationIds().get(0));
        Level jobServiceLogLevel = LogManager.getLogger(JobServiceImpl.class).getLevel();
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.WARN);
        try (PerformanceTimer timer = new PerformanceTimer("Finished exporting " + role + " to redshift.")) {
            JobStatus completedStatus = jobService.waitFinalJobStatus(submission.getApplicationIds().get(0), timeout);
            LogManager.getLogger(JobServiceImpl.class).setLevel(jobServiceLogLevel);
            Assert.assertEquals(completedStatus.getStatus(), FinalApplicationStatus.SUCCEEDED);
        }
        return true;
    }

    // Copied from ExportDataToRedshift
    private ExportConfiguration setupExportConfig(Table sourceTable, String targetTableName,
            TableRoleInCollection tableRole) {
        HdfsToRedshiftConfiguration exportConfig = new HdfsToRedshiftConfiguration();
        exportConfig.setExportFormat(ExportFormat.AVRO);
        exportConfig.setCleanupS3(true);
        exportConfig.setCreateNew(true);
        exportConfig.setAppend(true);
        exportConfig.setCustomerSpace(CustomerSpace.parse(mainTestTenant.getId()));
        exportConfig.setExportInputPath(sourceTable.getExtractsDirectory() + "/*.avro");
        exportConfig.setExportTargetPath(sourceTable.getName());
        exportConfig.setNoSplit(true);
        exportConfig.setExportDestination(ExportDestination.REDSHIFT);

        // all distributed on account id
        String distKey = tableRole.getPrimaryKey().name();
        List<String> sortKeys = new ArrayList<>(tableRole.getForeignKeysAsStringList());
        if (!sortKeys.contains(tableRole.getPrimaryKey().name())) {
            sortKeys.add(tableRole.getPrimaryKey().name());
        }
        RedshiftTableConfiguration.SortKeyType sortKeyType = sortKeys.size() == 1
                ? RedshiftTableConfiguration.SortKeyType.Compound : RedshiftTableConfiguration.SortKeyType.Interleaved;

        RedshiftTableConfiguration redshiftTableConfig = new RedshiftTableConfiguration();
        redshiftTableConfig.setS3Bucket(s3Bucket);
        redshiftTableConfig.setDistStyle(RedshiftTableConfiguration.DistStyle.Key);
        redshiftTableConfig.setDistKey(distKey);
        redshiftTableConfig.setSortKeyType(sortKeyType);
        redshiftTableConfig.setSortKeys(sortKeys);
        redshiftTableConfig.setTableName(targetTableName);
        redshiftTableConfig
                .setJsonPathPrefix(String.format("%s/jsonpath/%s.jsonpath", RedshiftUtils.AVRO_STAGE, targetTableName));
        exportConfig.setRedshiftTableConfiguration(redshiftTableConfig);

        return exportConfig;
    }

    List<Report> retrieveReport(String appId) {
        Job job = testBed.getRestTemplate().getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", deployedHostPort, appId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        return reports;
    }

    void verifyStats(BusinessEntity... entities) {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(container);
        Statistics statistics = container.getStatistics();
        for (BusinessEntity entity : entities) {
            switch (entity) {
            case Account:
                verifyAccountStats(statistics);
                break;
            case Contact:
                verifyContactStats(statistics);
                break;
            }
        }
    }

    private void verifyAccountStats(Statistics statistics) {
        Assert.assertTrue(statistics.hasCategory(Category.ACCOUNT_ATTRIBUTES));
        Assert.assertTrue(statistics.hasCategory(Category.FIRMOGRAPHICS));
        Assert.assertTrue(statistics.hasCategory(Category.ONLINE_PRESENCE));
        Assert.assertTrue(statistics.hasCategory(Category.WEBSITE_PROFILE));
        Assert.assertTrue(statistics.hasCategory(Category.TECHNOLOGY_PROFILE));
    }

    private void verifyContactStats(Statistics statistics) {
        Assert.assertTrue(statistics.hasCategory(Category.CONTACT_ATTRIBUTES));
    }

    void verifyConsolidateReport(String appId, Map<TableRoleInCollection, Long> expectedCounts) {
        List<Report> reports = retrieveReport(appId);
        // assertEquals(reports.size(), 3);
        Report publishReport = reports.get(0);
        verifyExportToRedshiftReport(publishReport, expectedCounts);
        Report summaryReport = reports.get(1);
        verifyConsolidateSummaryReport(summaryReport);
    }

    void verifyProfileReport(String appId, Map<TableRoleInCollection, Long> expectedCounts) {
        List<Report> reports = retrieveReport(appId);
        logger.info("Profil reports size " + reports.size());
        assertEquals(reports.size(), 1);
        Report publishReport = reports.get(0);
        verifyExportToRedshiftReport(publishReport, expectedCounts);
    }

    private void verifyConsolidateSummaryReport(Report summaryReport) {
        Assert.assertNotNull(summaryReport);
        Assert.assertNotNull(summaryReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(summaryReport.getJson().getPayload()));

        logger.info("ConsolidateSummaryReport: " + summaryReport.getJson().getPayload());
        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(summaryReport.getJson().getPayload());
            Assert.assertTrue(report.has(ReportPurpose.IMPORT_SUMMARY.getKey()));
            Assert.assertTrue(report.has(BusinessEntity.Account.name()));
            Assert.assertTrue(report.has(BusinessEntity.Contact.name()));
            Assert.assertTrue(report.has(BusinessEntity.Product.name()));
            Assert.assertTrue(report.has(BusinessEntity.Transaction.name()));
            ObjectNode accountReport = (ObjectNode) report.get(BusinessEntity.Account.name());
            Assert.assertNotNull(accountReport);
            Assert.assertTrue(accountReport.has("NEW"));
            Assert.assertTrue(accountReport.has("UPDATE"));
            Assert.assertTrue(accountReport.has("MATCH"));
            ObjectNode contactReport = (ObjectNode) report.get(BusinessEntity.Contact.name());
            Assert.assertNotNull(contactReport);
            Assert.assertTrue(contactReport.has("NEW"));
            Assert.assertTrue(contactReport.has("UPDATE"));
            ObjectNode productReport = (ObjectNode) report.get(BusinessEntity.Product.name());
            Assert.assertNotNull(productReport);
            Assert.assertTrue(productReport.has("NEW"));
            Assert.assertTrue(productReport.has("UPDATE"));
            ObjectNode transactionReport = (ObjectNode) report.get(BusinessEntity.Transaction.name());
            Assert.assertNotNull(transactionReport);
            Assert.assertTrue(transactionReport.has("TOTAL"));
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + summaryReport.getJson().getPayload(), e);
        }
    }

    private void verifyExportToRedshiftReport(Report publishReport, Map<TableRoleInCollection, Long> expectedCounts) {
        Map<String, Integer> map = JsonUtils.deserialize(publishReport.getJson().getPayload(),
                new TypeReference<Map<String, Integer>>() {
                });
        logger.info("Redshift report size is " + map.entrySet().size() + ", expected " + expectedCounts.size()
                + " reports for redshift exporting.");
        // assertEquals(map.entrySet().size(), expectedCounts.size(),
        // "Should have " + expectedCounts.size() + " reports for redshift
        // exporting.");
        expectedCounts.forEach((role, count) -> logger.info("Redshit report role " + role + " count "
                + map.get(role.name()).longValue() + " should have " + count.longValue()));
        expectedCounts.forEach((role, count) -> assertEquals(map.get(role.name()).longValue(), count.longValue(),
                "The count of table " + role + " does not meet the expectation."));
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
        System.out.println(JsonUtils.pprint(segment1));
        System.out.println(JsonUtils.pprint(segment2));
    }

    void createTestSegment1() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment1());
        MetadataSegment segment1 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_1);
        Assert.assertNotNull(segment1);
        System.out.println(JsonUtils.pprint(segment1));
    }

    void createTestSegment2() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment2());
        MetadataSegment segment2 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_2);
        Assert.assertNotNull(segment2);
        System.out.println(JsonUtils.pprint(segment2));
    }

    private MetadataSegment constructTestSegment1() {
        Bucket websiteBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList(".com"));
        BucketRestriction websiteRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, InterfaceName.Website.name()), websiteBkt);
        Bucket.Transaction txn = new Bucket.Transaction("A80D4770376C1226C47617C071324C0B", TimeFilter.ever(), null,
                null, false);
        Bucket purchaseBkt = Bucket.txnBkt(txn);
        BucketRestriction purchaseRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.PurchaseHistory, "AnyName"), purchaseBkt);
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

    private MetadataSegment constructTestSegment2() {
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
        ratingEngine.setCreatedBy("test@lattice-engines.com");
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

        this.ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
        return this.ratingEngine;
    }

    private RuleBasedModel constructRuleModel(String modelId) {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RuleBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt("CALIFORNIA");
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "LDC_State"), bktA);
        ratingRule.setRuleForBucket(RuleBucketName.A, resA, null);

        Bucket bktF = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("BOB"));
        Restriction resF = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.ContactName.name()), bktF);
        ratingRule.setRuleForBucket(RuleBucketName.F, null, resF);

        RuleBasedModel ruleBasedModel = new RuleBasedModel();
        ruleBasedModel.setRatingRule(ratingRule);
        ruleBasedModel.setId(modelId);
        return ruleBasedModel;
    }

    void verifyRatingEngineCount(String engineId, Map<RuleBucketName, Long> expectedCounts) {
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
            Assert.assertNotNull(counts.get(bkt.getName()),
                    "Cannot find count for bucket " + bkt.getName() + " in rating engine.");
            Assert.assertEquals(counts.get(bkt.getName()), count);
            logger.info("Rating engine count " + bkt.getName() + " expected " + counts.get(bkt.getName()) + " found "
                    + count);
        });
    }

    void verifyUpdateActions() {
        List<Action> actions = internalResourceProxy.findAll(mainTestTenant.getId());
        logger.info(String.format("actions=%s", actions));
        Assert.assertTrue(CollectionUtils.isNotEmpty(actions));
        Assert.assertTrue(actions.stream().allMatch(action -> action.getOwnerId() != null));
    }

}
