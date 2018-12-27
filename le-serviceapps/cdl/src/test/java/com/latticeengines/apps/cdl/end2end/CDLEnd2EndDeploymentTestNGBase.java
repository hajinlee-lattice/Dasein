package com.latticeengines.apps.cdl.end2end;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.Resource;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.apps.cdl.service.impl.CheckpointService;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.cache.exposed.service.CacheService;
import com.latticeengines.cache.exposed.service.CacheServiceBase;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cache.CacheName;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.cdl.ApsRollingPeriod;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.CSVImportConfig;
import com.latticeengines.domain.exposed.cdl.CSVImportFileInfo;
import com.latticeengines.domain.exposed.cdl.CleanupOperationType;
import com.latticeengines.domain.exposed.cdl.ModelingStrategy;
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
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActionType;
import com.latticeengines.domain.exposed.pls.CrossSellModelingConfigKeys;
import com.latticeengines.domain.exposed.pls.ImportActionConfiguration;
import com.latticeengines.domain.exposed.pls.ModelingConfigFilter;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
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
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BucketRestriction;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.ComparisonType;
import com.latticeengines.domain.exposed.query.Restriction;
import com.latticeengines.domain.exposed.query.TimeFilter;
import com.latticeengines.domain.exposed.query.frontend.FrontEndRestriction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;
import com.latticeengines.domain.exposed.serviceapps.cdl.BusinessCalendar;
import com.latticeengines.domain.exposed.util.ActivityMetricsTestUtils;
import com.latticeengines.domain.exposed.util.TimeSeriesUtils;
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

public abstract class CDLEnd2EndDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final Logger log = LoggerFactory.getLogger(CDLEnd2EndDeploymentTestNGBase.class);

    private static final int S3_CHECKPOINTS_VERSION = 22;
    private static final int S3_CROSS_SELL_CHECKPOINTS_VERSION = 20;

    private static final String INITIATOR = "test@lattice-engines.com";
    private static final String S3_VDB_DIR = "le-serviceapps/cdl/end2end/vdb";
    private static final String S3_VDB_VERSION = "2";

    protected static final String S3_CSV_DIR = "le-serviceapps/cdl/end2end/csv";
    protected static final String S3_CSV_VERSION = "4";

    private static final String S3_AVRO_DIR = "le-serviceapps/cdl/end2end/avro";
    private static final String S3_AVRO_VERSION = "4";

    private static final String LARGE_CSV_DIR = "le-serviceapps/cdl/end2end/large_csv";
    private static final String LARGE_CSV_VERSION = "1";

    static final Long ACCOUNT_1 = 500L;
    static final Long ACCOUNT_2 = 600L;
    static final Long ACCOUNT_3 = 1000L;
    static final Long CONTACT_1 = 500L;
    static final Long CONTACT_2 = 100L;
    static final Long CONTACT_3 = 1000L;
    static final Long PRODUCT_ID = 40L;
    static final Long PRODUCT_HIERARCHY = 5L;
    static final Long PRODUCT_BUNDLE = 14L;
    static final String PRODUCT_ERROR_MESSAGE = null;
    static final String PRODUCT_WARN_MESSAGE = "whatever warn message as it is not null or empty string";
    static final Long TRANSACTION_1 = 29264L;
    static final Long TRANSACTION_2 = 39004L;
    static final Long TRANSACTION_3 = 68268L;
    static final Long PURCHASE_HISTORY_1 = 5L;

    static final Long BATCH_STORE_PRODUCTS = 99L;
    static final Long SERVING_STORE_PRODUCTS = 30L;
    static final Long SERVING_STORE_PRODUCT_HIERARCHIES = 20L;

    static final String SEGMENT_NAME_1 = NamingUtils.timestamp("E2ESegment1");
    static final long SEGMENT_1_ACCOUNT_1 = 21;
    static final long SEGMENT_1_CONTACT_1 = 23;
    static final long SEGMENT_1_ACCOUNT_2 = 30;
    static final long SEGMENT_1_CONTACT_2 = 32;
    static final long SEGMENT_1_ACCOUNT_3 = 54;
    static final long SEGMENT_1_CONTACT_3 = 63;
    static final long SEGMENT_1_ACCOUNT_4 = 58;
    static final long SEGMENT_1_CONTACT_4 = 68;

    static final String SEGMENT_NAME_2 = NamingUtils.timestamp("E2ESegment2");
    static final long SEGMENT_2_ACCOUNT_1 = 13;
    static final long SEGMENT_2_CONTACT_1 = 13;
    static final long SEGMENT_2_ACCOUNT_2 = 45;
    static final long SEGMENT_2_CONTACT_2 = 49;
    static final long SEGMENT_2_ACCOUNT_2_REBUILD = 44;
    static final long SEGMENT_2_CONTACT_2_REBUILD = 49;

    static final String SEGMENT_NAME_3 = NamingUtils.timestamp("E2ESegment3");
    static final long SEGMENT_3_ACCOUNT_1 = 25;
    static final long SEGMENT_3_CONTACT_1 = 25;
    static final long SEGMENT_3_ACCOUNT_2 = 60;
    static final long SEGMENT_3_CONTACT_2 = 60;

    static final String SEGMENT_NAME_CURATED_ATTR = NamingUtils.timestamp("E2ESegmentCuratedAttr");
    static final String SEGMENT_NAME_MODELING = NamingUtils.timestamp("E2ESegmentModeling");
    static final String SEGMENT_NAME_TRAINING = NamingUtils.timestamp("E2ESegmentTraining");

    static final long RATING_A_COUNT_1 = 6;
    static final long RATING_D_COUNT_1 = 5;
    static final long RATING_F_COUNT_1 = 2;

    static final String TARGET_PRODUCT = "A48F113437D354134E584D8886116989";
    static final String TRAINING_PRODUCT = "9IfG2T5joqw0CIJva0izeZXSCwON1S";

    static final int EARLIEST_TRANSACTION = 48033;
    static final int LATEST_TRANSACTION = 48929;

    // 1: after 1st import (rebuild); 2: after 2nd import (update)
    static final int DAILY_TRANSACTION_DAYS1 = 114;
    static final int DAILY_TRANSACTION_DAYS2 = 260;
    static final String MIN_TRANSACTION_DATE1 = "2016-03-15";
    static final String MAX_TRANSACTION_DATE1 = "2017-06-02";
    static final String MIN_TRANSACTION_DATE2 = "2016-03-15";
    static final String MAX_TRANSACTION_DATE2 = "2017-12-31";

    static final String VERIFY_DAILYTXN_ACCOUNTID = "10";
    static final String VERIFY_DAILYTXN_PRODUCTID = "650050C066EF46905EC469E9CC2921E0";
    // After 1st import (rebuild), verify date = 2017-06-03
    // After 2nd import (update), 3 values will be doubled because 2nd import
    // has same transactions as 1st import for VERIFY_ACCOUNTID &
    // VERIFY_PRODUCTID
    static final double VERIFY_DAILYTXN_AMOUNT1 = 20850;
    static final double VERIFY_DAILYTXN_QUANTITY1 = 30;
    static final double VERIFY_DAILYTXN_COST = 5552.123186000001;

    int actionsNumber;

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
    protected ActionProxy actionProxy;

    @Inject
    private ActivityMetricsProxy activityMetricsProxy;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    protected String s3Bucket;

    @Value("${common.le.environment}")
    private String leEnv;

    @javax.annotation.Resource(name = "localCacheService")
    private CacheService localCacheService;

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
        log.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();

        log.info("Test environment setup finished.");
        createDataFeed();
        setupBusinessCalendar();
        setupPurchaseHistoryMetrics();
        setDefaultAPSRollupPeriod();

        attachProtectedProxy(fileUploadProxy);
        attachProtectedProxy(testMetadataSegmentProxy);

        // If don't want to remove testing tenant for debug purpose, remove
        // comments on this line but don't check in
        // testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
    }

    protected void resetCollection() {
        log.info("Start reset collection data ...");
        boolean resetStatus = cdlProxy.reset(mainTestTenant.getId());
        Assert.assertTrue(resetStatus);
    }

    protected void clearCache() {
        String tenantId = CustomerSpace.parse(mainCustomerSpace).getTenantId();
        CacheService cacheService = CacheServiceBase.getCacheService();
        cacheService.refreshKeysByPattern(tenantId, CacheName.getCdlCacheGroup());
        localCacheService.refreshKeysByPattern(tenantId, CacheName.getCdlLocalCacheGroup());
    }

    void processAnalyze() {
        processAnalyze(null);
    }

    void processAnalyze(ProcessAnalyzeRequest request) {
        log.info("Start processing and analyzing ...");
        ApplicationId appId = cdlProxy.processAnalyze(mainTestTenant.getId(), request);
        processAnalyzeAppId = appId.toString();
        log.info("processAnalyzeAppId=" + processAnalyzeAppId);
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(),
                false);
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
    }

    SourceFile uploadDeleteCSV(String fileName, SchemaInterpretation schema, CleanupOperationType type,
            Resource source) {
        log.info("Upload file " + fileName + ", operation type is " + type.name() + ", Schema is " + schema.name());
        return fileUploadProxy.uploadDeleteFile(false, fileName, schema.name(), type.name(), source);
    }

    void mockCSVImport(BusinessEntity entity, int fileIdx, String feedType) {
        List<String> strings = registerMockDataFeedTask(entity, feedType);
        String feedTaskId = strings.get(0);
        String templateName = strings.get(1);
        Date now = new Date();
        String fileName = String.format("%s-%d.avro", entity.name(), fileIdx);
        InputStream is = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, S3_AVRO_VERSION, fileName);
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String extractPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.FILE.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(now));
        long numRecords;
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, is, extractPath + "/part-00000.avro");
            numRecords = AvroUtils.count(yarnConfiguration, extractPath + "/*.avro");
            log.info("Uploaded " + numRecords + " records from " + fileName + " to " + extractPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro file " + fileName);
        }
        Extract extract = new Extract();
        extract.setName("Extract-" + templateName);
        extract.setPath(extractPath);
        extract.setProcessedRecords(numRecords);
        extract.setExtractionTimestamp(now.getTime());
        List<String> tableNames = dataFeedProxy.registerExtract(customerSpace.toString(), feedTaskId, templateName,
                extract);
        registerImportAction(feedTaskId, numRecords, tableNames);
    }

    private Table getMockTemplate(BusinessEntity entity, String feedType) {
        String templateFileName = String.format("%s_%s.json", entity.name(), feedType);
        InputStream templateIs = testArtifactService.readTestArtifactAsStream(S3_AVRO_DIR, S3_AVRO_VERSION,
                templateFileName);
        ObjectMapper om = new ObjectMapper();
        try {
            return om.readValue(templateIs, Table.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + entity.name() + " template from S3.");
        }
    }

    private List<String> registerMockDataFeedTask(BusinessEntity entity, String feedType) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String feedTaskId;
        String templateName = NamingUtils.timestamp(entity.name());
        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), SourceType.FILE.getName(),
                feedType, entity.name());
        if (dataFeedTask == null) {
            dataFeedTask = new DataFeedTask();
            Table importTemplate = getMockTemplate(entity, feedType);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            importTemplate.setName(templateName);
            dataFeedTask.setImportTemplate(importTemplate);

            dataFeedTask.setStatus(DataFeedTask.Status.Active);
            dataFeedTask.setEntity(entity.name());
            dataFeedTask.setFeedType(feedType);
            dataFeedTask.setSource(SourceType.FILE.getName());
            dataFeedTask.setActiveJob("Not specified");
            dataFeedTask.setSourceConfig("Not specified");
            dataFeedTask.setStartTime(new Date());
            dataFeedTask.setLastImported(new Date(0L));
            dataFeedTask.setLastUpdated(new Date());
            dataFeedTask.setUniqueId(NamingUtils.uuid("DataFeedTask"));
            dataFeedProxy.createDataFeedTask(customerSpace.toString(), dataFeedTask);
            feedTaskId = dataFeedTask.getUniqueId();
        } else {
            feedTaskId = dataFeedTask.getUniqueId();
            templateName = dataFeedTask.getImportTemplate().getName();

        }
        return Arrays.asList(feedTaskId, templateName);
    }

    private void registerImportAction(String feedTaskId, long count, List<String> tableNames) {
        log.info(String.format("Registering action for dataFeedTask=%s", feedTaskId));
        ImportActionConfiguration configuration = new ImportActionConfiguration();
        configuration.setDataFeedTaskId(feedTaskId);
        configuration.setImportCount(count);
        configuration.setRegisteredTables(tableNames);
        configuration.setMockCompleted(true);
        Action action = new Action();
        action.setType(ActionType.CDL_DATAFEED_IMPORT_WORKFLOW);
        action.setActionInitiator(INITIATOR);
        action.setDescription(feedTaskId);
        action.setTrackingPid(null);
        action.setActionConfiguration(configuration);
        actionProxy.createAction(mainCustomerSpace, action);
    }

    void importData(BusinessEntity entity, String s3FileName, String feedType) {
        importData(entity, s3FileName, feedType, false, false);
    }

    void importData(BusinessEntity entity, String s3FileName, String feedType, boolean compressed,
            boolean outsizeFlag) {
        Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(s3FileName, outsizeFlag), s3FileName);
        log.info("Streaming S3 file " + s3FileName + " as a template file for " + entity);
        String outputFileName = s3FileName;
        if (StringUtils.isBlank(feedType)) {
            feedType = entity.name() + "Schema";
        }
        if (s3FileName.endsWith(".gz"))
            outputFileName = s3FileName.substring(0, s3FileName.length() - 3);
        SourceFile template = fileUploadProxy.uploadFile(outputFileName, compressed, s3FileName, entity.name(),
                csvResource, outsizeFlag);
        FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(), entity.name(),
                SourceType.FILE.getName(), feedType);
        modifyFieldMappings(entity, fieldMappingDocument);
        for (FieldMapping fieldMapping : fieldMappingDocument.getFieldMappings()) {
            if (fieldMapping.getMappedField() == null) {
                fieldMapping.setMappedField(fieldMapping.getUserField());
                fieldMapping.setMappedToLatticeField(false);
            }
        }

        fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument, entity.name(),
                SourceType.FILE.getName(), feedType);
        log.info("Modified field mapping document is saved, start importing ...");
        ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), feedType, template, template,
                INITIATOR);
        JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
        Assert.assertEquals(status, JobStatus.COMPLETED);
        log.info("Importing S3 file " + s3FileName + " for " + entity + " is finished.");
    }

    void importData(BusinessEntity entity, List<String> s3FileName, String feedType, boolean compressed,
            boolean outsizeFlag) {
        List<ApplicationId> applicationIds = new ArrayList<>();
        if (StringUtils.isBlank(feedType)) {
            feedType = entity.name() + "Schema";
        }
        for (String filename : s3FileName) {
            Resource csvResource = new MultipartFileResource(readCSVInputStreamFromS3(filename, outsizeFlag), filename);
            log.info("Streaming S3 file " + filename + " as a template file for " + entity);
            String outputFileName = filename;
            if (filename.endsWith(".gz"))
                outputFileName = filename.substring(0, filename.length() - 3);
            SourceFile template = fileUploadProxy.uploadFile(outputFileName, compressed, filename, entity.name(),
                    csvResource, outsizeFlag);
            FieldMappingDocument fieldMappingDocument = fileUploadProxy.getFieldMappings(template.getName(),
                    entity.name(), SourceType.FILE.getName(), feedType);
            modifyFieldMappings(entity, fieldMappingDocument);
            fieldMappingDocument.getFieldMappings().stream().forEach(fieldMapping -> {
                if (fieldMapping.getMappedField() == null) {
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);
                }
            });

            fileUploadProxy.saveFieldMappingDocument(template.getName(), fieldMappingDocument, entity.name(),
                    SourceType.FILE.getName(), feedType);
            log.info("Modified field mapping document is saved, start importing ...");
            ApplicationId applicationId = submitImport(mainTestTenant.getId(), entity.name(), feedType, template,
                    template, INITIATOR);
            applicationIds.add(applicationId);
        }
        int count = 0;
        for (ApplicationId applicationId : applicationIds) {
            JobStatus status = waitForWorkflowStatus(applicationId.toString(), false);
            Assert.assertEquals(status, JobStatus.COMPLETED);
            log.info("Importing S3 file " + s3FileName.get(count) + " for " + entity + " is finished.");
            count++;
        }
    }

    private void modifyFieldMappings(BusinessEntity entity, FieldMappingDocument fieldMappingDocument) {
        switch (entity) {
        case Account:
            modifyFieldMappingsForAccount(fieldMappingDocument);
            break;
        default:
        }
    }

    private void modifyFieldMappingsForAccount(FieldMappingDocument fieldMappingDocument) {
        setExternalSystem(fieldMappingDocument.getFieldMappings());
        setDateAttributes(fieldMappingDocument.getFieldMappings());
    }

    private void setExternalSystem(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
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
    }

    private void setDateAttributes(List<FieldMapping> fieldMappings) {
        for (FieldMapping fieldMapping : fieldMappings) {
            if (fieldMapping.getMappedField() == null) {
                if (fieldMapping.getUserField().equalsIgnoreCase("Test Date")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateTimeFormatString("MM/DD/YY");
                    fieldMapping.setTimezone("UTC");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.error("$JAW$ Setting Test Date field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 2")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateTimeFormatString("YYYY-MM-DD");
                    fieldMapping.setTimezone("America/Los_Angeles");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.error("$JAW$ Setting Test Date 2 field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 3")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateTimeFormatString("DD.MM.YY 00:00:00 24H");
                    fieldMapping.setTimezone("GMT+8");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.error("$JAW$ Setting Test Date 3 field mapping.");
                } else if (fieldMapping.getUserField().equalsIgnoreCase("Test Date 4")) {
                    fieldMapping.setFieldType(UserDefinedType.DATE);
                    fieldMapping.setDateTimeFormatString("MM/DD/YYYY 00-00-00 12H");
                    fieldMapping.setTimezone("Asia/Kolkata");
                    fieldMapping.setMappedField(fieldMapping.getUserField());
                    fieldMapping.setMappedToLatticeField(false);

                    log.error("$JAW$ Setting Test Date 4 field mapping.");
                }
            }
        }
    }

    private ApplicationId submitImport(String customerSpace, String entity, String feedType,
            SourceFile templateSourceFile, SourceFile dataSourceFile, String email) {
        String source = SourceType.FILE.getName();
        CSVImportConfig metaData = generateImportConfig(customerSpace, templateSourceFile, dataSourceFile, email);
        String taskId = cdlProxy.createDataFeedTask(customerSpace, SourceType.FILE.getName(), entity, feedType, "", "",
                metaData);
        log.info("Creating a data feed task for " + entity + " with id " + taskId);
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
        return readCSVInputStreamFromS3(fileName, false);
    }

    private InputStream readCSVInputStreamFromS3(String fileName, boolean outsizeFlag) {
        if (outsizeFlag)
            return testArtifactService.readTestArtifactAsStream(LARGE_CSV_DIR, LARGE_CSV_VERSION, fileName);
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

    void verifyActionRegistration() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<Action> actions = actionProxy.getActionsByOwnerId(customerSpace.toString(), null);
        // Assert.assertEquals(actions.size(), ++actionsNumber);
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

    long countTableRole(TableRoleInCollection role, DataCollection.Version version) {
        return checkpointService.countTableRole(role, version);
    }

    long countInRedshift(BusinessEntity entity) {
        return checkpointService.countInRedshift(entity);
    }

    String getTableName(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        return dataCollectionProxy.getTableName(customerSpace.toString(), role);
    }

    void resumeCrossSellCheckpoint(String checkpoint) throws IOException {
        checkpointService.resumeCheckpoint(checkpoint, S3_CROSS_SELL_CHECKPOINTS_VERSION);
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
    }

    void resumeCheckpoint(String checkpoint) throws IOException {
        checkpointService.resumeCheckpoint(checkpoint, S3_CHECKPOINTS_VERSION);
        initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
    }

    void saveCheckpoint(String checkpoint) throws IOException {
        checkpointService.saveCheckPoint(checkpoint, mainCustomerSpace);
    }

    private List<Report> retrieveReport(String appId) {
        Job job = testBed.getRestTemplate().getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", deployedHostPort, appId), //
                Job.class);
        assertNotNull(job);
        return job.getReports();
    }

    Map<String, StatsCube> verifyStats(boolean onlyAllowSpecifiedEntities, BusinessEntity... entities) {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(container);
        Map<String, StatsCube> cubeMap = container.getStatsCubes();
        for (BusinessEntity entity : entities) {
            Assert.assertTrue(cubeMap.containsKey(entity.name()), "Stats should contain a cube for " + entity);
        }

        if (onlyAllowSpecifiedEntities) {
            Set<BusinessEntity> allowed = new HashSet<>(Arrays.asList(entities));
            for (BusinessEntity entity : BusinessEntity.values()) {
                if (!allowed.contains(entity)) {
                    Assert.assertFalse(cubeMap.containsKey(entity.name()),
                            "Stats not should contain a cube for " + entity);
                }
            }
        }
        return cubeMap;
    }

    void verifyDataCollectionStatus(DataCollection.Version version) {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        Assert.assertTrue(dataCollectionStatus.getAccountCount() > 0);
    }

    void verifyProcessAnalyzeReport(String appId, Map<BusinessEntity, Map<String, Object>> expectedReport) {
        List<Report> reports = retrieveReport(appId);
        assertEquals(reports.size(), 1);
        Report summaryReport = reports.get(0);
        verifySystemActionReport(summaryReport);
        verifyConsolidateSummaryReport(summaryReport, expectedReport);
    }

    private void verifySystemActionReport(Report summaryReport) {
        Assert.assertNotNull(summaryReport);
        Assert.assertNotNull(summaryReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(summaryReport.getJson().getPayload()));
        log.info("SystemActionReport: " + summaryReport.getJson().getPayload());

        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(summaryReport.getJson().getPayload());
            ArrayNode systemActionNode = (ArrayNode) report.get(ReportPurpose.SYSTEM_ACTIONS.getKey());
            Assert.assertNotNull(systemActionNode);
            for (JsonNode n : systemActionNode) {
                Assert.assertNotNull(n);
            }
        } catch (IOException e) {
            throw new RuntimeException("Fail to parse report payload: " + summaryReport.getJson().getPayload(), e);
        }
    }

    private void verifyConsolidateSummaryReport(Report summaryReport,
            Map<BusinessEntity, Map<String, Object>> expectedReport) {
        Assert.assertNotNull(summaryReport);
        Assert.assertNotNull(summaryReport.getJson());
        Assert.assertTrue(StringUtils.isNotBlank(summaryReport.getJson().getPayload()));
        log.info("ConsolidateSummaryReport: " + summaryReport.getJson().getPayload());

        try {
            ObjectMapper om = JsonUtils.getObjectMapper();
            ObjectNode report = (ObjectNode) om.readTree(summaryReport.getJson().getPayload());
            ObjectNode entitiesSummaryNode = (ObjectNode) report.get(ReportPurpose.ENTITIES_SUMMARY.getKey());

            expectedReport.forEach((entity, entityReport) -> {
                Assert.assertTrue(entitiesSummaryNode.has(entity.name()));
                ObjectNode entityNode = (ObjectNode) entitiesSummaryNode.get(entity.name());
                Assert.assertNotNull(entityNode);
                ObjectNode consolidateSummaryNode = (ObjectNode) entityNode
                        .get(ReportPurpose.CONSOLIDATE_RECORDS_SUMMARY.getKey());
                Assert.assertNotNull(consolidateSummaryNode);
                ObjectNode entityNumberNode = (ObjectNode) entityNode.get(ReportPurpose.ENTITY_STATS_SUMMARY.getKey());
                if (entity != BusinessEntity.Product) {
                    Assert.assertNotNull(entityNumberNode);
                }

                entityReport.forEach((reportKey, reportValue) -> {
                    String[] keySplits = reportKey.split("_");
                    if (keySplits[0].equals(ReportPurpose.ENTITIES_SUMMARY.name())) {
                        Assert.assertTrue(consolidateSummaryNode.has(keySplits[1]));
                        if (reportValue instanceof Long) {
                            Assert.assertEquals(consolidateSummaryNode.get(keySplits[1]).asLong(), reportValue);
                        } else if (reportValue instanceof String) {
                            Assert.assertFalse(consolidateSummaryNode.get(keySplits[1]).isNull());
                        }
                    } else if (keySplits[0].equals(ReportPurpose.ENTITY_STATS_SUMMARY.name())) {
                        Assert.assertTrue(entityNumberNode.has(keySplits[1]));
                        Assert.assertEquals(entityNumberNode.get(keySplits[1]).asLong(), reportValue);
                    }
                });
            });
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

    void createTestSegment3() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegment3());
        MetadataSegment segment3 = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_3);
        Assert.assertNotNull(segment3);
    }

    void createTestSegmentCuratedAttr() {
        testMetadataSegmentProxy.createOrUpdate(constructTestSegmentCuratedAttr());
        MetadataSegment segmentCuratedAttr = testMetadataSegmentProxy.getSegment(SEGMENT_NAME_CURATED_ATTR);
        Assert.assertNotNull(segmentCuratedAttr);
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
        Bucket.Transaction txn = new Bucket.Transaction("GMm4ZQnMOWpN8Gn7MhZLB7SrGmOss", TimeFilter.ever(), null, null,
                false);
        Bucket purchaseBkt = Bucket.txnBkt(txn);
        BucketRestriction purchaseRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Transaction, "AnyName"), purchaseBkt);
        Restriction accountRestriction = Restriction.builder().and(websiteRestriction, purchaseRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("Engineer"));
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
        BucketRestriction stateRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"),
                stateBkt);
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("SEGMENT_5"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "SpendAnalyticsSegment"), techBkt);
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

    protected MetadataSegment constructTestSegment3() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.IN_COLLECTION,
                Arrays.asList("CALIFORNIA", "TEXAS", "MICHIGAN", "NEW YORK"));
        BucketRestriction stateRestriction = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"),
                stateBkt);
        Bucket techBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList("General Practice"));
        BucketRestriction techRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Account, "SpendAnalyticsSegment"), techBkt);
        Restriction accountRestriction = Restriction.builder().or(stateRestriction, techRestriction).build();

        Bucket titleBkt = Bucket.valueBkt(ComparisonType.CONTAINS, Collections.singletonList("Manager"));
        BucketRestriction titleRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.Contact, InterfaceName.Title.name()), titleBkt);
        Restriction contactRestriction = Restriction.builder().and(titleRestriction).build();

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_3);
        segment.setDisplayName("End2End Segment 3");
        segment.setDescription("A test segment for CDL end2end tests.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(accountRestriction));
        segment.setContactFrontEndRestriction(new FrontEndRestriction(contactRestriction));

        return segment;
    }

    protected MetadataSegment constructTestSegmentCuratedAttr() {
        Bucket numberOfContactsBkt = Bucket.valueBkt(ComparisonType.EQUAL, Collections.singletonList(1));
        BucketRestriction numberOfContactsRestriction = new BucketRestriction(
                new AttributeLookup(BusinessEntity.CuratedAccount, InterfaceName.NumberOfContacts.name()),
                numberOfContactsBkt);

        MetadataSegment segment = new MetadataSegment();
        segment.setName(SEGMENT_NAME_CURATED_ATTR);
        segment.setDisplayName("End2End Segment Curated Attributes");
        segment.setDescription("A test segment for CDL end2end tests checking the number of contacts.");
        segment.setAccountFrontEndRestriction(new FrontEndRestriction(numberOfContactsRestriction));
        return segment;
    }

    MetadataSegment constructTargetSegment() {
        Bucket stateBkt = Bucket.valueBkt(ComparisonType.NOT_IN_COLLECTION, Collections.singletonList("Delaware"));
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

    void verifyTestSegment1Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_1, expectedCounts);
    }

    void verifyTestSegment2Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_2, expectedCounts);
    }

    void verifyTestSegment3Counts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_3, expectedCounts);
    }

    void verifyTestSegmentCuratedAttrCounts(Map<BusinessEntity, Long> expectedCounts) {
        verifySegmentCounts(SEGMENT_NAME_CURATED_ATTR, expectedCounts);
    }

    private void verifySegmentCounts(String segmentName, Map<BusinessEntity, Long> expectedCounts) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        expectedCounts.forEach((entity, count) -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            Assert.assertEquals(immutableSegment.getEntityCount(entity), count, JsonUtils.pprint(immutableSegment));
        });
    }

    Map<BusinessEntity, Long> getSegmentCounts(String segmentName, Set<BusinessEntity> entities) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        Map<BusinessEntity, Long> cnts = new HashMap<>();
        entities.forEach(entity -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            cnts.put(entity, immutableSegment.getEntityCount(entity));
        });
        return cnts;
    }

    void verifySegmentCountsNonNegative(String segmentName, Collection<BusinessEntity> entities) {
        final MetadataSegment immutableSegment = getSegmentByName(segmentName);
        entities.forEach(entity -> {
            Assert.assertNotNull(immutableSegment.getEntityCount(entity), "Cannot find count of " + entity);
            Assert.assertTrue(immutableSegment.getEntityCount(entity) > 0, JsonUtils.pprint(immutableSegment));
        });
    }

    void verifySegmentCountsIncreased(Map<BusinessEntity, Long> segmentCnts,
            Map<BusinessEntity, Long> segmentCntsUpdated) {
        Assert.assertTrue(MapUtils.isNotEmpty(segmentCnts));
        Assert.assertTrue(MapUtils.isNotEmpty(segmentCntsUpdated));
        Assert.assertEquals(segmentCnts.size(), segmentCntsUpdated.size());
        segmentCnts.forEach((entity, cnt) -> {
            Assert.assertNotNull(cnt);
            Assert.assertNotNull(segmentCntsUpdated.get(entity));
            Assert.assertTrue(cnt.longValue() < segmentCntsUpdated.get(entity).longValue());
        });
    }

    private MetadataSegment getSegmentByName(String segmentName) {
        MetadataSegment segment = testMetadataSegmentProxy.getSegment(segmentName);
        int retries = 0;
        while (segment == null && retries++ < 3) {
            log.info("Wait for 1 sec to retry getting rating engine.");
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                // ignore
            }
            segment = testMetadataSegmentProxy.getSegment(segmentName);
        }
        Assert.assertNotNull(segment,
                "Cannot find rating engine " + segmentName + " in tenant " + mainTestTenant.getId());
        log.info(String.format("Get segment %s:\n%s", segmentName, JsonUtils.serialize(segment)));
        return segment;
    }

    @SuppressWarnings("deprecation")
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
        Assert.assertNotNull(newEngine.getLatestIteration(), JsonUtils.pprint(newEngine));

        String modelId = newEngine.getLatestIteration().getId();
        RuleBasedModel model = constructRuleModel(modelId);
        ratingEngineProxy.updateRatingModel(mainTestTenant.getId(), newEngine.getId(), modelId, model);
        return ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), newEngine.getId());
    }

    void activateRatingEngine(String engineId) {
        activateRatingEngine(engineId, mainTestTenant);
    }

    private RuleBasedModel constructRuleModel(String modelId) {
        RatingRule ratingRule = new RatingRule();
        ratingRule.setDefaultBucketName(RatingBucketName.D.getName());

        Bucket bktA = Bucket.valueBkt("CALIFORNIA");
        Restriction resA = new BucketRestriction(new AttributeLookup(BusinessEntity.Account, "State"), bktA);
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

    void configureCrossSellModel(AIModel testAIModel, PredictionType predictionType, ModelingStrategy strategy,
            List<String> targetProducts, List<String> trainingProducts) {
        testAIModel.setPredictionType(predictionType);
        CrossSellModelingConfig config = CrossSellModelingConfig.getAdvancedModelingConfig(testAIModel);
        config.setModelingStrategy(strategy);
        Map<CrossSellModelingConfigKeys, ModelingConfigFilter> myMap = new HashMap<>();
        if (ModelingStrategy.CROSS_SELL_REPEAT_PURCHASE.equals(strategy)) {
            myMap.put(CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, new ModelingConfigFilter(
                    CrossSellModelingConfigKeys.PURCHASED_BEFORE_PERIOD, ComparisonType.PRIOR_ONLY, 1));
        }
        config.setFilters(myMap);
        config.setTargetProducts(targetProducts);
        config.setTrainingProducts(trainingProducts);
    }

    void configureCustomEventModel(AIModel testAIModel, String sourceFileName, CustomEventModelingType type) {
        CustomEventModelingConfig advancedConf = CustomEventModelingConfig.getAdvancedModelingConfig(testAIModel);
        if (type == CustomEventModelingType.CDL)
            advancedConf.setDataStores(Arrays.asList(CustomEventModelingConfig.DataStore.CDL,
                    CustomEventModelingConfig.DataStore.DataCloud));
        else {
            advancedConf.setDataStores(Arrays.asList(CustomEventModelingConfig.DataStore.CustomFileAttributes,
                    CustomEventModelingConfig.DataStore.DataCloud));
        }
        advancedConf.setCustomEventModelingType(type);
        advancedConf.setDeduplicationType(DedupType.ONELEADPERDOMAIN);
        advancedConf.setExcludePublicDomains(false);
        advancedConf.setSourceFileName(sourceFileName);
        advancedConf.setSourceFileDisplayName(sourceFileName);
        advancedConf.setTransformationGroup(null); // TransformationGroup.ALL
    }

    void verifyRatingEngineCount(String engineId, Map<RatingBucketName, Long> expectedCounts) {
        RatingEngine ratingEngine = ratingEngineProxy.getRatingEngine(mainTestTenant.getId(), engineId);
        int retries = 0;
        while (ratingEngine == null && retries++ < 3) {
            log.info("Wait for 1 sec to retry getting rating engine.");
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
        // Map<String, Long> counts = ratingEngine.getCountsAsMap();
        // Assert.assertTrue(MapUtils.isNotEmpty(counts));
        // expectedCounts.forEach((bkt, count) -> {
        // if (count > 0) {
        // Assert.assertNotNull(counts.get(bkt.getName()),
        // "Cannot find count for bucket " + bkt.getName() + " in rating
        // engine.");
        // Assert.assertEquals(counts.get(bkt.getName()), count, "Rating engine
        // count " + bkt.getName()
        // + " expected " + counts.get(bkt.getName()) + " found " + count);
        // }
        // });
    }

    List<ColumnMetadata> getFullyDecoratedMetadata(BusinessEntity entity) {
        return servingStoreProxy.getDecoratedMetadataFromCache(mainCustomerSpace, entity);
    }

    void verifyAccountFeatures() {
        String tableName = dataCollectionProxy.getTableName(mainCustomerSpace, TableRoleInCollection.AccountFeatures);
        Assert.assertNotNull(tableName);
        List<ColumnMetadata> cms = metadataProxy.getTableColumns(mainCustomerSpace, tableName);
        List<ColumnMetadata> amCols = columnMetadataProxy.columnSelection(ColumnSelection.Predefined.Model);
        String msg = String.format("AccountFeatures has %d columns while AM has %d columns in the Model group.", //
                CollectionUtils.size(cms), CollectionUtils.size(amCols));
        Assert.assertTrue(CollectionUtils.size(cms) > CollectionUtils.size(amCols) + 1, msg);
    }

    void verifyUpdateActions() {
        List<Action> actions = actionProxy.getActions(mainTestTenant.getId());
        log.info(String.format("actions=%s", actions));
        Assert.assertTrue(CollectionUtils.isNotEmpty(actions));
        Assert.assertTrue(actions.stream().allMatch(action -> action.getOwnerId() != null));
    }

    void verifyBatchStore(Map<BusinessEntity, Long> expectedEntityCount) {
        expectedEntityCount
                .forEach((key, value) -> Assert.assertEquals(Long.valueOf(countTableRole(key.getBatchStore())), value));
    }

    void verifyRedshift(Map<BusinessEntity, Long> expectedEntityCount) {
        expectedEntityCount.forEach((key, value) -> Assert.assertEquals(Long.valueOf(countInRedshift(key)), value));
    }

    void verifyServingStore(Map<BusinessEntity, Long> expectedEntityCount) {
        expectedEntityCount.forEach((key, value) -> {
            if (key != BusinessEntity.ProductHierarchy) {
                Assert.assertEquals(Long.valueOf(countInRedshift(key)), value);
            } else {
                Assert.assertEquals(
                        Long.valueOf(periodTransactionProxy.getProductHierarchy(mainCustomerSpace, null).size()),
                        value);
            }
        });
    }

    void runCommonPAVerifications() {
        verifyDataFeedStatus(DataFeed.Status.Active);
        verifyActiveVersion(initialVersion.complement());
        verifyDataCollectionStatus(initialVersion.complement());
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
        List<ActivityMetrics> metrics = ActivityMetricsTestUtils.fakePurchaseMetrics(mainTestTenant);
        activityMetricsProxy.save(mainCustomerSpace, ActivityType.PurchaseHistory, metrics);
    }

    void setDefaultAPSRollupPeriod() {
        String rollupPeriod = ApsRollingPeriod.BUSINESS_QUARTER.getName();
        String podId = CamilleEnvironment.getPodId();
        Path zkPath = PathBuilder.buildCustomerSpaceServicePath(podId, CustomerSpace.parse(mainCustomerSpace), "CDL");
        zkPath = zkPath.append("DefaultAPSRollupPeriod");
        Camille camille = CamilleEnvironment.getCamille();
        try {
            camille.upsert(zkPath, new Document(rollupPeriod), ZooDefs.Ids.OPEN_ACL_UNSAFE);
        } catch (Exception e) {
            throw new RuntimeException("Failed to update DefaultAPSRollupPeriod", e);
        }
        log.info("Updated DefaultAPSRollupPeriod to " + rollupPeriod);
    }

    boolean isLocalEnvironment() {
        return "dev".equals(leEnv);
    }

    void verifyTxnDailyStore(int totalDays, int minDay, int maxDay, double amount, double quantity, double cost)
            throws IOException {
        DataCollection.Version activeVersion = dataCollectionProxy.getActiveVersion(mainCustomerSpace);
        Table dailyTable = dataCollectionProxy.getTable(mainCustomerSpace,
                TableRoleInCollection.ConsolidatedDailyTransaction, activeVersion);
        // Verify number of days
        List<String> dailyFiles = HdfsUtils.getFilesForDir(yarnConfiguration, dailyTable.getExtractsDirectory());
        Assert.assertEquals(dailyFiles.size(), totalDays);
        // Verify max/min day period
        Pair<Integer, Integer> minMaxPeriods = TimeSeriesUtils.getMinMaxPeriod(yarnConfiguration, dailyTable);
        Assert.assertEquals((int) minMaxPeriods.getLeft(), minDay);
        Assert.assertEquals((int) minMaxPeriods.getRight(), maxDay);
        // Verify daily aggregated result
        int dayPeriod = DateTimeUtils.dateToDayPeriod(MAX_TRANSACTION_DATE1);
        Iterator<GenericRecord> iter = AvroUtils.iterator(yarnConfiguration, dailyFiles.stream()
                .filter(f -> f.contains(String.valueOf(dayPeriod))).collect(Collectors.toList()).get(0));
        GenericRecord verifyRecord = null;
        while (iter.hasNext()) {
            GenericRecord record = iter.next();
            if (VERIFY_DAILYTXN_ACCOUNTID.equals(record.get(InterfaceName.AccountId.name()).toString())
                    && VERIFY_DAILYTXN_PRODUCTID.equals(record.get(InterfaceName.ProductId.name()).toString())) {
                verifyRecord = record;
                break;
            }
        }
        Assert.assertNotNull(verifyRecord);
        log.info("Verified record: " + verifyRecord.toString());
        Assert.assertEquals((double) verifyRecord.get(InterfaceName.TotalAmount.name()), amount);
        Assert.assertEquals((double) verifyRecord.get(InterfaceName.TotalQuantity.name()), quantity);
        Assert.assertEquals((double) verifyRecord.get(InterfaceName.TotalCost.name()), cost);
    }

}
