package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.AvroParquetUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchEnvironment;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishRequest;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityPublishStatistics;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollectionStatus;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecutionJobType;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.DynamoDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.RedshiftDataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftPartitionService;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.workflowapi.service.WorkflowJobService;
import com.latticeengines.yarn.exposed.service.EMREnvService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

public abstract class CheckpointServiceBase {

    private static final Logger log = LoggerFactory.getLogger(CheckpointServiceBase.class);

    protected static final String S3_CHECKPOINTS_DIR = "le-serviceapps/cdl/end2end/checkpoints";
    protected static final String S3_BUCKET = "latticeengines-test-artifacts";

    @Inject
    protected DataCollectionProxy dataCollectionProxy;

    @Inject
    protected DataFeedProxy dataFeedProxy;

    @Inject
    protected DataFeedService dataFeedService;

    @Inject
    protected Configuration yarnConfiguration;

    @Inject
    protected MetadataProxy metadataProxy;

    @Inject
    protected EntityProxy entityProxy;

    @Inject
    protected TestArtifactService testArtifactService;

    @Inject
    protected RedshiftPartitionService redshiftPartitionService;

    @Inject
    protected DataUnitProxy dataUnitProxy;

    @Inject
    protected ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected WorkflowJobService workflowJobService;

    @Inject
    protected MatchProxy matchProxy;

    @Inject
    protected BatonService batonService;

    @Inject
    protected EMREnvService emrEnvService;

    @Inject
    protected S3Service s3Service;

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${common.le.stack}")
    protected String leStack;

    @Value("${common.test.matchapi.url}")
    protected String matchapiHostPort;

    @Value("${aws.customer.s3.bucket}")
    protected String s3CustomerBucket;

    @Value("${hadoop.use.emr}")
    protected Boolean useEmr;

    protected ObjectMapper om = new ObjectMapper();

    protected Tenant mainTestTenant;

    protected String checkpointDir;

    // For entity match enabled PA, when saving checkpoint for match lookup/seed
    // table, need to publish all the preceding checkpoints' staging lookup/seed
    // table instead of only current tenant.
    // Reason is in current tenant's staging table, only entries which are
    // touched in match job exist which means the staging table doesn't have
    // complete entity universe for the tenant. Although serving table has
    // complete entity universe, serving table doesn't support scan due to
    // lookup performance concern.
    protected List<String> precedingCheckpoints;

    protected Map<TableRoleInCollection, String> savedRedshiftTables = new HashMap<>();

    protected boolean copyToS3 = false;

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    public Tenant getMainTestTenant() {
        return this.mainTestTenant;
    }

    public void setPrecedingCheckpoints(List<String> precedingCheckpoints) {
        this.precedingCheckpoints = precedingCheckpoints;
    }

    public List<String> getPrecedingCheckpoints() {
        return this.precedingCheckpoints;
    }

    public void enableCopyToS3() {
        copyToS3 = true;
    }

    public void resumeCheckpoint(String checkpoint, int checkpointVersion) throws IOException {
        resumeCheckpoint(checkpoint, String.valueOf(checkpointVersion));
    }

    public void resumeCheckpoint(String checkpoint, String checkpointVersion) throws IOException {
        unzipCheckpoint(checkpoint, checkpointVersion);
        cloneAnUploadTables(checkpoint, checkpointVersion);
        updateDataCloudBuildNumber();
    }

    public abstract void saveCheckpoint(String checkpointName, String checkpointVersion, String customerSpace)
            throws IOException;

    protected abstract void cloneAnUploadTables(String checkpoint, String checkpointVersion) throws IOException;

    protected void cloneRedshiftTables(Map<String, String> redshiftTablesToClone) {
        if (MapUtils.isNotEmpty(redshiftTablesToClone)) {
            RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
            ThreadPoolUtils.doInParallel(redshiftTablesToClone.entrySet(), entry -> {
                String src = entry.getKey();
                String tgt = entry.getValue();
                String msg = "Clone redshift table " + src + " to " + tgt;
                try (PerformanceTimer timer = new PerformanceTimer(msg)) {
                    redshiftService.cloneTable(src, tgt);
                    RedshiftDataUnit dataUnit = new RedshiftDataUnit();
                    dataUnit.setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                    dataUnit.setName(tgt);
                    dataUnit.setRedshiftTable(tgt.toLowerCase());
                    dataUnit.setClusterPartition(redshiftPartitionService.getDefaultPartition());
                    dataUnitProxy.create(mainTestTenant.getId(), dataUnit);
                } catch (Exception e) {
                    throw new RuntimeException("Failed to clone redshift table.", e);
                }
            });
        }
    }

    protected void unzipCheckpoint(String checkpoint, String version) throws IOException {
        checkpointDir = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        File downloadedFile = testArtifactService.downloadTestArtifact(S3_CHECKPOINTS_DIR, version,
                checkpoint + ".zip");
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(checkpointDir);
        } catch (ZipException e) {
            throw new IOException("Failed to unzip checkpoint archive " + zipFilePath, e);
        }
        log.info(String.format("Unzip checkpoint archive %s to local dir %s", zipFilePath, checkpointDir));
    }

    public long countTableRole(TableRoleInCollection role) {
        return countTableRole(role, null);
    }

    public long countTableRole(TableRoleInCollection role, DataCollection.Version version) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<Table> tables = dataCollectionProxy.getTables(customerSpace.toString(), role, version);
        if (CollectionUtils.isEmpty(tables)) {
            Assert.fail("Cannot find table in role " + role);
        }
        return tables.stream().mapToLong(table -> {
            log.info("countRole " + role.name() + " Table " + table.getName() + " Path "
                    + table.getExtracts().get(0).getPath());
            String path = table.getExtracts().get(0).getPath();
            String globPath = AvroParquetUtils.toParquetOrAvroGlob(yarnConfiguration, path);
            return AvroParquetUtils.countParquetOrAvro(yarnConfiguration, globPath);
        }).sum();
    }

    public long countInRedshift(BusinessEntity entity) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);
        return entityProxy.getCount(mainTestTenant.getId(), frontEndQuery);
    }

    @SuppressWarnings("unused")
    protected void verifyStatistics() {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId());
        Assert.assertNotNull(statisticsContainer);
        Map<String, StatsCube> cubeMap = statisticsContainer.getStatsCubes();
        cubeMap.values().forEach(cube -> cube.getStatistics().forEach(this::verifyAttrStats));
    }

    private void verifyAttrStats(String attrName, AttributeStats attributeStats) {
        Assert.assertNotNull(attributeStats.getNonNullCount());
        Assert.assertTrue(attributeStats.getNonNullCount() >= 0);
        Buckets buckets = attributeStats.getBuckets();
        if (buckets != null) {
            Assert.assertNotNull(buckets.getType());
            Assert.assertFalse(buckets.getBucketList() == null || buckets.getBucketList().isEmpty(),
                    "Bucket list for " + attrName + " is empty.");
            Long sum = buckets.getBucketList().stream().mapToLong(Bucket::getCount).sum();
            Assert.assertEquals(sum, attributeStats.getNonNullCount());
        }
    }

    protected void uploadCheckpointHdfs(String checkpoint) throws IOException {
        log.info("Start uploading checkpoint " + checkpoint + " to hdfs.");
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildDataPath(podId, cs).toString();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localDir, targetPath);
        log.info("Upload checkpoint to hdfs path " + targetPath);
    }

    protected void uploadCheckpointS3(String checkpoint) {
        log.info("Start uploading checkpoint " + checkpoint + " to S3.");
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
        String s3Prefix = pathBuilder.getS3AtlasDataPrefix(s3CustomerBucket, cs.getTenantId());
        if (s3Service.isNonEmptyDirectory(s3CustomerBucket, s3Prefix)) {
            log.info("S3 prefix {} already exists, delete first.", s3Prefix);
            s3Service.cleanupPrefix(s3CustomerBucket, s3Prefix);
        }
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        s3Service.uploadLocalDirectory(s3CustomerBucket, s3Prefix, localDir, true);
    }

    protected DataCollection.Version getCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = String.format("%s/%s/_VERSION_", checkpointDir, checkpoint);
        String version = FileUtils.readFileToString(new File(versionFile), "UTF-8").trim();
        return DataCollection.Version.valueOf(version);
    }

    protected DynamoDataUnit parseDynamoDataUnit(String checkpoint, String roleName, DataCollection.Version version)
            throws IOException {
        DynamoDataUnit dynamoDataUnit = null;
        String jsonFilePath = String.format("%s/%s/%s/%s_Dynamo.json", checkpointDir, checkpoint, version.name(),
                roleName);
        File jsonFile = new File(jsonFilePath);
        if (jsonFile.exists()) {
            log.info("Found dynamo data unit for " + roleName + " in " + version);
            dynamoDataUnit = om.readValue(jsonFile, DynamoDataUnit.class);
        }
        return dynamoDataUnit;
    }

    protected StatisticsContainer parseCheckpointStatistics(String checkpoint, DataCollection.Version version)
            throws IOException {
        String statsFile = String.format("%s/%s/%s/stats_container.json", checkpointDir, checkpoint, version.name());
        if (!new File(statsFile).exists()) {
            return null;
        }
        StatisticsContainer statisticsContainer = JsonUtils.deserialize(new FileInputStream(statsFile),
                StatisticsContainer.class);
        statisticsContainer.setName(NamingUtils.timestamp("Stats"));
        return statisticsContainer;
    }

    protected DataCollectionStatus parseDataCollectionStatus(String checkpoint, DataCollection.Version version)
            throws IOException {
        String jsonFile = String.format("%s/%s/%s/data_collection_status.json", checkpointDir, checkpoint,
                version.name());
        if (!new File(jsonFile).exists()) {
            return null;
        }
        DataCollectionStatus status = JsonUtils.deserialize(new FileInputStream(jsonFile), DataCollectionStatus.class);
        status.setRedshiftPartition(redshiftPartitionService.getDefaultPartition());
        return status;
    }

    public void cleanup() {
        if (StringUtils.isNotBlank(checkpointDir)) {
            FileUtils.deleteQuietly(new File(checkpointDir));
        }
    }

    protected void resumeDbState() {
        log.info("Resuming DB state.");
        Long tenantPid = getTenantPid();
        Long feedPid = getDataFeedPid(tenantPid);
        Long workflowId = createFakeWorkflow(tenantPid);
        Long executionId = createExecution(feedPid, workflowId);
        updateActiveExecution(feedPid, executionId);

        createFakeWorkflow(tenantPid);
    }

    private Long getTenantPid() {
        String sql = "SELECT `TENANT_PID` FROM `TENANT` WHERE ";
        sql += String.format("`TENANT_ID` = '%s'", mainTestTenant.getId());
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    private Long createFakeWorkflow(long tenantPid) {
        int rand = new Random(System.currentTimeMillis()).nextInt(10000);
        String appId = String.format("application_%d_%04d", System.currentTimeMillis(), rand);

        String sql = "INSERT INTO `WORKFLOW_JOB` ";
        sql += "(`TENANT_ID`, `FK_TENANT_ID`, `USER_ID`, `APPLICATION_ID`, `INPUT_CONTEXT`, `START_TIME`) VALUES ";
        sql += String.format("(%d, %d, 'DEFAULT_USER', '%s', '{}', NOW())", tenantPid, tenantPid, appId);
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `WORKFLOW_JOB` WHERE ";
        sql += String.format("`TENANT_ID` = %d", tenantPid);
        sql += String.format(" AND `APPLICATION_ID` = '%s'", appId);
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        log.info("Created a fake workflow " + pid);
        return pid;
    }

    private Long createExecution(long feedPid, long workflowPid) {
        String sql = "INSERT INTO `DATAFEED_EXECUTION` ";
        sql += "(`FK_FEED_ID`, `WORKFLOW_ID`, `STATUS`, `JOB_TYPE`) VALUES ";
        sql += String.format("(%d, %d, '%s', '%s')", feedPid, workflowPid, DataFeedExecution.Status.Completed.name(),
                DataFeedExecutionJobType.PA.name());
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `DATAFEED_EXECUTION` WHERE `FK_FEED_ID` = " + feedPid;
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        log.info("Created a fake execution " + pid);
        return pid;
    }

    private Long getDataFeedPid(long tenantPid) {
        String sql = "SELECT `PID` FROM `DATAFEED` WHERE ";
        sql += String.format("`FK_TENANT_ID` = %d", tenantPid);
        return jdbcTemplate.queryForObject(sql, Long.class);
    }

    private void updateActiveExecution(Long feedPid, Long executionId) {
        String sql = "UPDATE `DATAFEED` ";
        sql += String.format("SET `ACTIVE_EXECUTION` = %d ", executionId);
        sql += String.format("WHERE `PID` = %d", feedPid);
        jdbcTemplate.execute(sql);
        log.info("Set execution " + executionId + " as active execution of data feed " + feedPid);
    }

    protected void updateDataCloudBuildNumber() {
        String currentDataCloudBuildNumber = columnMetadataProxy.latestBuildNumber();
        DataCollection.Version initialVersion = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        DataCollectionStatus status = dataCollectionProxy.getOrCreateDataCollectionStatus(mainTestTenant.getId(),
                initialVersion);
        status.setDataCloudBuildNumber(currentDataCloudBuildNumber);
        dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestTenant.getId(), status, initialVersion);
        log.info(String.format("Update datacloud rebuild number as %s for tenant %s with version %s",
                currentDataCloudBuildNumber, mainTestTenant.getId(), initialVersion));
    }

    protected void saveRedshiftTableIfExists(TableRoleInCollection role, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(mainTestTenant.getId(), role, version);
        RedshiftService redshiftService = redshiftPartitionService.getBatchUserService(null);
        if (StringUtils.isNotBlank(tableName)) {
            List<String> redshiftTables = redshiftService.getTables(tableName);
            if (CollectionUtils.isNotEmpty(redshiftTables)) {
                if (redshiftTables.size() != 1) {
                    throw new IllegalStateException("There are " + redshiftTables.size()
                            + " redshift tables prefixed by " + tableName + ": " + redshiftTables);
                }
                String oldTable = redshiftTables.get(0);
                String newTable = String.format("%s_%s", TestFrameworkUtils.generateTenantName(), role.name());
                newTable = NamingUtils.timestamp(newTable);
                redshiftService.dropTable(newTable);
                redshiftService.renameTable(oldTable, newTable);
                savedRedshiftTables.put(role, newTable);
            }
        }
    }

    protected void saveDynamoTableIfExists(String checkpoint, TableRoleInCollection role, DataCollection.Version version)
            throws IOException {
        String tableName = dataCollectionProxy.getTableName(mainTestTenant.getId(), role, version);
        if (StringUtils.isNotBlank(tableName)) {
            DataUnit dataUnit = dataUnitProxy.getByNameAndType(mainTestTenant.getId(), tableName,
                    DataUnit.StorageType.Dynamo);
            if (dataUnit != null) {
                DynamoDataUnit dynamoDataUnit = (DynamoDataUnit) dataUnit;
                String jsonFile = String.format("checkpoints/%s/%s/%s_Dynamo.json", checkpoint, version.name(),
                        role.name());
                om.writeValue(new File(jsonFile), dynamoDataUnit);
                log.info("Save DynamoDataUnit for " + role + " at version " + version + " to " + jsonFile);
            }
        }
    }

    protected void saveStatsIfExists(DataCollection.Version version, String checkpoint) throws IOException {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId(), version);
        if (statisticsContainer != null) {
            String jsonFile = String.format("checkpoints/%s/%s/stats_container.json", checkpoint, version.name());
            om.writeValue(new File(jsonFile), statisticsContainer);
            log.info("Save stats at version " + version + " to " + jsonFile);
        } else {
            log.info("There is no stats at version " + version);
        }
    }

    protected void saveCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = "checkpoints/" + checkpoint + "/_VERSION_";
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        FileUtils.write(new File(versionFile), version.name(), "UTF-8");
    }

    protected void downloadHdfsData(String checkpoint) throws IOException {
        String localDir = "checkpoints/" + checkpoint + "/hdfs/Data";
        FileUtils.deleteQuietly(new File(localDir));
        FileUtils.forceMkdirParent(new File(localDir));
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data").toString();
        HdfsUtils.copyHdfsToLocal(yarnConfiguration, targetPath, localDir);
        log.info("Downloaded HDFS path " + targetPath + " to " + localDir);
        Collection<File> crcFiles = FileUtils.listFiles(new File(localDir), new String[] { "crc" }, true);
        crcFiles.forEach(FileUtils::deleteQuietly);
    }

    protected void saveTableIfExists(TableRoleInCollection role, DataCollection.Version version, String checkpoint)
            throws IOException {
        List<Table> tables = new ArrayList<>();
        if (role == TableRoleInCollection.ConsolidatedPeriodTransaction) {
            tables = dataCollectionProxy.getTables(mainTestTenant.getId(), role, version);
        } else {
            tables.add(dataCollectionProxy.getTable(mainTestTenant.getId(), role, version));
        }
        if (CollectionUtils.isNotEmpty(tables) && tables.get(0) != null) {
            String jsonFile = String.format("checkpoints/%s/%s/tables/%s.json", checkpoint, version.name(),
                    role.name());
            om.writeValue(new File(jsonFile), tables);
            log.info("Save " + role + " at version " + version + " to " + jsonFile);
        } else {
            log.info("There is no " + role + " table at version " + version);
        }
    }

    protected String checkpointRedshiftTableName(String checkpoint, TableRoleInCollection role,
                                               String checkpointVersion) {
        return String.format("cdlend2end_%s_%s_%s", checkpoint, role.name(), checkpointVersion);
    }

    protected void saveWorkflowExecutionContext(String checkpoint, String customerSpace) throws IOException {
        // Get the workflow ID from the customer space using DataFeed.
        DataFeed dataFeed = dataFeedService.getDefaultDataFeed(customerSpace);
        DataFeedExecution dataFeedExecution = dataFeed.getActiveExecution();
        Long workflowId = dataFeedExecution.getWorkflowId();
        ExecutionContext executionContext = workflowJobService.getExecutionContextByWorkflowId(customerSpace,
                workflowId);

        if (executionContext == null) {
            log.error("Failed to get execution context");
        } else {
            // Strip out keys we don't need to save. For now, those are the keys that are not all capitals and end
            // in "Configuration" or "Workflow". These can be regenerated when the workflow is restarted from the
            // checkpoint.
            Set<Map.Entry<String, Object>> executionContextMap = executionContext.entrySet();
            for (Map.Entry<String, Object> mapEntry : executionContextMap) {
                if (mapEntry.getKey().endsWith("Configuration") || mapEntry.getKey().endsWith("Workflow")) {
                    executionContextMap.remove(mapEntry);
                }
            }
            String jsonFile = String.format("checkpoints/%s/workflow_execution_context.json", checkpoint);
            om.writeValue(new File(jsonFile), executionContextMap);
            log.info("Save Workflow Execution Context to " + jsonFile);
        }
    }

    protected void copyEntitySeedTables(String checkpoint, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            log.info("Tenant {} is not an entity match tenant, skip copying entity seed/lookup tables",
                    mainTestTenant.getId());
            return;
        }
        ThreadPoolUtils.doInParallel(Arrays.asList( //
                BusinessEntity.Account.name(), //
                BusinessEntity.Contact.name() //
        ), entity -> copyEntitySeedTable(checkpoint, checkpointVersion, entity));
    }

    private void copyEntitySeedTable(String checkpoint, String checkpointVersion, String entity) {
        EntityPublishRequest request = new EntityPublishRequest();
        request.setEntity(entity);
        String srcTenantId = getCheckPointTenantId(checkpoint, checkpointVersion, entity);
        Tenant srcTenant = new Tenant(CustomerSpace.parse(srcTenantId).toString());
        request.setSrcTenant(srcTenant);
        request.setDestTenant(mainTestTenant);
        request.setDestEnv(EntityMatchEnvironment.SERVING);
        request.setDestTTLEnabled(true);
        request.setBumpupVersion(false);
        log.info("Start copying entity match table for " + entity + " using request: " + JsonUtils.serialize(request));
        EntityPublishStatistics stats = matchProxy.publishEntity(request);
        log.info("Copied {} {} seeds and {} {} lookup entries from tenant {} to tenant {}", stats.getSeedCount(),
                entity, //
                stats.getLookupCount(), entity, //
                CustomerSpace.shortenCustomerSpace(srcTenant.getId()), //
                CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
    }

    public static String getCheckPointTenantId(String checkpoint, String checkpointVersion, String entity) {
        return "cdlend2end_" + checkpoint + "_" + entity.toLowerCase() + "_" + checkpointVersion;
    }

    @VisibleForTesting
    boolean isEntityMatchEnabled() {
        FeatureFlagValueMap flags = batonService.getFeatureFlags(CustomerSpace.parse(mainTestTenant.getId()));
        boolean emEnabled = FeatureFlagUtils.isEntityMatchEnabled(flags);
        log.info("Feature flags = {}, isEntityMatch={}", flags, emEnabled);
        return emEnabled;
    }
}
