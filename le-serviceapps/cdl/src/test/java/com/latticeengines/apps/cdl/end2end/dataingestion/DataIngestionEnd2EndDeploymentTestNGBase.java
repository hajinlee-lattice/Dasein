package com.latticeengines.apps.cdl.end2end.dataingestion;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
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
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.ExportConfiguration;
import com.latticeengines.domain.exposed.eai.ExportDestination;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.HdfsToRedshiftConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.TableType;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.domain.exposed.metadata.statistics.Statistics;
import com.latticeengines.domain.exposed.pls.EntityExternalType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.redshift.RedshiftTableConfiguration;
import com.latticeengines.domain.exposed.util.MetadataConverter;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.Report;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.redshiftdb.exposed.utils.RedshiftUtils;
import com.latticeengines.testframework.exposed.proxy.pls.ModelingFileUploadProxy;
import com.latticeengines.yarn.exposed.service.JobService;
import com.latticeengines.yarn.exposed.service.impl.JobServiceImpl;

public abstract class DataIngestionEnd2EndDeploymentTestNGBase extends CDLDeploymentTestNGBase {

    private static final String COLLECTION_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";
    private static final Logger logger = LoggerFactory.getLogger(DataIngestionEnd2EndDeploymentTestNGBase.class);

    @Inject
    DataCollectionProxy dataCollectionProxy;

    @Inject
    DataFeedProxy dataFeedProxy;

    @Inject
    CDLProxy cdlProxy;

    @Inject
    private EaiProxy eaiProxy;

    @Inject
    private JobService jobService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private ModelingFileUploadProxy fileUploadProxy;

    @Inject
    private CheckpointService checkpointService;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${aws.s3.bucket}")
    private String s3Bucket;

    protected String consolidateAppId;
    protected String profileAppId;

    @BeforeClass(groups = { "end2end" })
    public void setup() throws Exception {
        logger.info("Bootstrapping test tenants using tenant console ...");

        setupTestEnvironment();
        mainTestTenant = testBed.getMainTestTenant();
        // testBed.excludeTestTenantsForCleanup(Collections.singletonList(mainTestTenant));
        checkpointService.setMaintestTenant(mainTestTenant);

        logger.info("Test environment setup finished.");
        createDataFeed();

        attachProtectedProxy(fileUploadProxy);
    }

    @AfterClass(groups = { "end2end" })
    protected void cleanup() throws Exception {
        checkpointService.cleanup();
    }

    protected void resetCollection() {
        logger.info("Start reset collection data ...");
        boolean resetStatus = cdlProxy.reset(mainTestTenant.getId());
        assertEquals(resetStatus, true);
    }

    protected void consolidate() {
        logger.info("Start consolidating ...");
        ApplicationId appId = cdlProxy.consolidate(mainTestTenant.getId());
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
        consolidateAppId = appId.toString();
    }

    protected void profile() throws IOException {
        logger.info("Start profiling ...");
        ApplicationId appId = cdlProxy.profile(mainTestTenant.getId());
        com.latticeengines.domain.exposed.workflow.JobStatus completedStatus = waitForWorkflowStatus(appId.toString(), false);
        assertEquals(completedStatus, com.latticeengines.domain.exposed.workflow.JobStatus.COMPLETED);
        profileAppId = appId.toString();
    }

    protected long mockCsvImport(BusinessEntity entity, int fileId) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query",
                entity.name());
        Table importTemplate;
        if (dataFeedTask == null) {
            Schema schema = getCsvImportSchema(entity);
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            if (BusinessEntity.Account.equals(entity)) {
                importTemplate.setName(SchemaInterpretation.Account.name());
            } else {
                importTemplate.setName(SchemaInterpretation.Contact.name());
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

        String targetPath = uploadMockedCsvImportData(entity, fileId);
        String defaultFS = yarnConfiguration.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        long count = AvroUtils.count(yarnConfiguration, targetPath + "/*.avro");
        Extract e = createExtract(hdfsUri, count);
        dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query", entity.name());
        dataFeedProxy.registerExtract(customerSpace.toString(), dataFeedTask.getUniqueId(), importTemplate.getName(),
                e);
        return count;
    }

    protected Schema getCsvImportSchema(BusinessEntity entity) throws IOException {
        InputStream avscIs = Thread.currentThread().getContextClassLoader().getResourceAsStream(String.format("end2end/csv/%s.avsc", entity.name()));
        return new Schema.Parser().parse(avscIs);
    }

    protected String uploadMockedCsvImportData(BusinessEntity entity, int fileId) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-" + entity + "/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()));
        InputStream dataIs = Thread.currentThread().getContextClassLoader().getResourceAsStream(String.format("end2end/csv/%s%d.avro", entity.name(), fileId));
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dataIs, targetPath + "/part-00000.avro");
            logger.info(String.format("Uploaded %s records to %s", entity.name(), targetPath + "/part-00000.avro"));
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro for " + entity);
        }
        return targetPath;
    }


    protected void uploadAccountCSV() {
        Resource csvResrouce = new ClassPathResource("end2end/csv/Account1.csv", Thread.currentThread().getContextClassLoader());
        SourceFile sourceFile = fileUploadProxy.uploadFile("Account1.csv", false, "Account1.csv", SchemaInterpretation.Account,
                EntityExternalType.Account, csvResrouce);
        logger.info("Uploaded file " + sourceFile.getName() + " to " + sourceFile.getPath());
    }


    protected void mockVdbImport(BusinessEntity entity, int offset, int limit) throws IOException {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());

        DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace.toString(), "VisiDB", "Query",
                entity.name());
        Table importTemplate;
        if (dataFeedTask == null) {
            Schema schema = getVdbImportSchema(entity);
            importTemplate = MetadataConverter.getTable(schema, new ArrayList<>(), null, null, false);
            importTemplate.setTableType(TableType.IMPORTTABLE);
            if (BusinessEntity.Account.equals(entity)) {
                importTemplate.setName(SchemaInterpretation.Account.name());
            } else {
                importTemplate.setName(SchemaInterpretation.Contact.name());
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
    }

    private Schema getVdbImportSchema(BusinessEntity entity) {
        Schema schema;
        try {
            InputStream schemaIs = Thread.currentThread().getContextClassLoader()
                    .getResourceAsStream("end2end/vdb/Account.avsc");
            String schemaStr = IOUtils.toString(schemaIs, Charset.forName("UTF-8"));
            switch (entity) {
            case Contact:
                schemaStr = schemaStr.replace("\"LEAccountIDLong\"", "\"" + InterfaceName.AccountId.name() + "\"");
                break;
            case Account:
            default:
            }

            schema = new Schema.Parser().parse(schemaStr);
            boolean hasId;
            boolean hasAccountId;
            switch (entity) {
            case Contact:
                hasId = schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.Id.name().equals(n));
                hasAccountId = schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.AccountId.name().equals(n));
                Assert.assertTrue(hasId);
                Assert.assertTrue(hasAccountId);
                break;
            case Account:
                hasId= schema.getFields().stream().map(Schema.Field::name)
                        .anyMatch(n -> InterfaceName.Id.name().equals(n));
                Assert.assertTrue(hasId);
                break;
            default:
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to prepare avro schema for " + entity);
        }
        return schema;
    }

    private String uploadMockDataWithModifiedSchema(BusinessEntity entity, int offset, int limit) {
        Schema schema = getVdbImportSchema(entity);
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-" + entity + "/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(COLLECTION_DATE_FORMAT).format(new Date()));
        InputStream dataIs = Thread.currentThread().getContextClassLoader().getResourceAsStream("end2end/vdb/Account.avro");
        try {
            List<GenericRecord> records = AvroUtils.readFromInputStream(dataIs);
            AvroUtils.writeToHdfsFile(yarnConfiguration, schema, targetPath + "/part-00000.avro",
                    records.subList(offset, offset + limit), true);
            logger.info("Uploaded " + limit + " records to " + targetPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to upload avro for " + entity);
        }
        return targetPath;
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

    protected long countTableRole(TableRoleInCollection role) {
        return checkpointService.countTableRole(role);
    }

    protected long countInRedshift(BusinessEntity entity) {
        return checkpointService.countInRedshift(entity);
    }

    protected void verifyFirstProfileCheckpoint() throws IOException {
        checkpointService.verifyFirstProfileCheckpoint();
    }

    protected void verifySecondConsolidateCheckpoint() throws IOException {
        checkpointService.verifySecondConsolidateCheckpoint();
    }

    protected void verifySecondProfileCheckpoint() throws IOException {
        checkpointService.verifySecondProfileCheckpoint();
    }

    protected void resumeCheckpoint(String checkpoint) throws IOException {
        checkpointService.resumeCheckpoint(checkpoint);
    }

    protected void exportEntityToRedshift(BusinessEntity entity) {
        TableRoleInCollection role = entity.getServingStore();
        logger.info("Started exporting " + role + " to redshift ...");
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), role);
        ExportConfiguration exportConfiguration = setupExportConfig(table, table.getName(), role);
        AppSubmission submission = eaiProxy.submitEaiJob(exportConfiguration);
        int timeout = new Long(TimeUnit.MINUTES.toSeconds(30)).intValue();
        logger.info("Waiting for " + submission.getApplicationIds().get(0));
        Level jobServiceLogLevel = LogManager.getLogger(JobServiceImpl.class).getLevel();
        LogManager.getLogger(JobServiceImpl.class).setLevel(Level.WARN);
        JobStatus completedStatus = jobService.waitFinalJobStatus(submission.getApplicationIds().get(0), timeout);
        LogManager.getLogger(JobServiceImpl.class).setLevel(jobServiceLogLevel);
        Assert.assertEquals(completedStatus.getStatus(), FinalApplicationStatus.SUCCEEDED);
        logger.info("Finished exporting " + role + " to redshift.");
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

    protected Report retrieveReport(String appId) {
        Job job = testBed.getRestTemplate().getForObject( //
                String.format("%s/pls/jobs/yarnapps/%s", deployedHostPort, appId), //
                Job.class);
        assertNotNull(job);
        List<Report> reports = job.getReports();
        assertEquals(reports.size(), 1);
        return reports.get(0);
    }

    protected void verifyStats(BusinessEntity... entities) {
        StatisticsContainer container = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(container);
        Statistics statistics = container.getStatistics();
        for (BusinessEntity entity: entities) {
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

    protected void verifyReport(String appId, int reportSize, Long exportedAccounts, Long exportedContacts) {
        verifyReport(appId, reportSize, exportedAccounts.intValue(), exportedContacts.intValue());
    }

    protected void verifyReport(String appId, int reportSize, int exportedAccounts, int exportedContacts) {
        Report report = retrieveReport(appId);
        Map<String, Integer> map = JsonUtils.deserialize(report.getJson().getPayload(),
                new TypeReference<Map<String, Integer>>() {
                });
        assertEquals(map.entrySet().size(), reportSize);
        if (reportSize != 0) {
            assertEquals(map.get(TableRoleInCollection.BucketedAccount.name()).intValue(), exportedAccounts);
            assertEquals(map.get(TableRoleInCollection.SortedContact.name()).intValue(), exportedContacts);
        }
    }

}
