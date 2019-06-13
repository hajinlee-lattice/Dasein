package com.latticeengines.apps.cdl.service.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.apps.cdl.service.DataFeedService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
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
import com.latticeengines.domain.exposed.datacloud.match.entity.BumpVersionRequest;
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
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.DataFeedProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.workflowapi.service.WorkflowJobService;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@Component("checkpointService")
public class CheckpointService {

    private static final Logger log = LoggerFactory.getLogger(CheckpointService.class);

    private static final String S3_CHECKPOINTS_DIR = "le-serviceapps/cdl/end2end/checkpoints";

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

    @Inject
    private DataFeedService dataFeedService;

    @Inject
    private Configuration yarnConfiguration;

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private EntityProxy entityProxy;

    @Inject
    private TestArtifactService testArtifactService;

    @Inject
    private RedshiftService redshiftService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private WorkflowJobService workflowJobService;

    @Inject
    private MatchProxy matchProxy;

    @Inject
    private BatonService batonService;

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${common.le.stack}")
    private String leStack;

    @Value("${common.test.matchapi.url}")
    private String matchapiHostPort;

    private ObjectMapper om = new ObjectMapper();

    private Tenant mainTestTenant;

    private String checkpointDir;

    // For entity match enabled PA, when saving checkpoint for match lookup/seed
    // table, need to publish all the preceding checkpoints' staging lookup/seed
    // table instead of only current tenant.
    // Reason is in current tenant's staging table, only entries which are
    // touched in match job exist which means the staging table doesn't have
    // complete entity universe for the tenant. Although serving table has
    // complete entity universe, serving table doesn't support scan due to
    // lookup performance concern.
    private List<String> precedingCheckpoints;

    private Map<TableRoleInCollection, String> savedRedshiftTables = new HashMap<>();

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    public void setPrecedingCheckpoints(List<String> precedingCheckpoints) {
        this.precedingCheckpoints = precedingCheckpoints;
    }

    public void resumeCheckpoint(String checkpoint, int checkpointVersion) throws IOException {
        resumeCheckpoint(checkpoint, String.valueOf(checkpointVersion));
    }

    public void resumeCheckpoint(String checkpoint, String checkpointVersion) throws IOException {
        unzipCheckpoint(checkpoint, checkpointVersion);
        cloneAnUploadTables(checkpoint, checkpointVersion);
        updateDataCloudBuildNumber();
    }

    private void cloneAnUploadTables(String checkpoint, String checkpointVersion) throws IOException {
        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        String[] tenantNames = new String[1];

        DataCollection.Version activeVersion = getCheckpointVersion(checkpoint);

        Map<String, String> redshiftTablesToClone = new HashMap<>();
        Set<String> uploadedTables = new HashSet<>();
        for (DataCollection.Version version : DataCollection.Version.values()) {
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                List<Table> tables = parseCheckpointTable(checkpoint, role.name(), version, tenantNames);
                List<String> tableNames = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(tables)) {
                    for (Table table : tables) {
                        if (table != null) {
                            log.info("Creating table " + table.getName() + " for " + role + " in version " + version);
                            if (!uploadedTables.contains(table.getName())) {
                                metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
                                uploadedTables.add(table.getName());
                            }
                            tableNames.add(table.getName());
                            if (activeVersion.equals(version)) {
                                String redshiftTable = checkpointRedshiftTableName(checkpoint, role, checkpointVersion);
                                if (redshiftService.hasTable(redshiftTable)) {
                                    redshiftTablesToClone.put(redshiftTable, table.getName());
                                }
                                DynamoDataUnit dynamoDataUnit = parseDynamoDataUnit(checkpoint, role.name(), version);
                                if (dynamoDataUnit != null) {
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTable())) {
                                        dynamoDataUnit.setLinkedTable(dynamoDataUnit.getName());
                                    }
                                    if (StringUtils.isBlank(dynamoDataUnit.getLinkedTenant())) {
                                        dynamoDataUnit.setLinkedTenant(dynamoDataUnit.getTenant());
                                    }
                                    dynamoDataUnit.setName(table.getName());
                                    dynamoDataUnit
                                            .setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                                    log.info("Creating data unit " + JsonUtils.serialize(dynamoDataUnit));
                                    dataUnitProxy.create(mainTestTenant.getId(), dynamoDataUnit);
                                }
                            }
                        }
                    }
                }

                if (CollectionUtils.isNotEmpty(tableNames)) {
                    if (tableNames.size() == 1) {
                        dataCollectionProxy.upsertTable(mainTestTenant.getId(), tableNames.get(0), role, version);
                    } else {
                        dataCollectionProxy.upsertTables(mainTestTenant.getId(), tableNames, role, version);
                    }
                }
            }
            StatisticsContainer statisticsContainer = parseCheckpointStatistics(checkpoint, version);
            if (statisticsContainer != null) {
                dataCollectionProxy.upsertStats(mainTestTenant.getId(), statisticsContainer);
            }
            DataCollectionStatus dataCollectionStatus = parseDataCollectionStatus(checkpoint, version);
            if (dataCollectionStatus != null) {
                dataCollectionProxy.saveOrUpdateDataCollectionStatus(mainTestTenant.getId(), dataCollectionStatus,
                        version);
            }
        }

        cloneRedshiftTables(redshiftTablesToClone);
        uploadCheckpointHdfs(checkpoint);

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Active.name());
        resumeDbState();

        copyEntitySeedTables(checkpoint, checkpointVersion);

        dataCollectionProxy.switchVersion(mainTestTenant.getId(), activeVersion);
        log.info("Switch active version to " + activeVersion);
    }

    private void cloneRedshiftTables(Map<String, String> redshiftTablesToClone) {
        if (MapUtils.isEmpty(redshiftTablesToClone)) {
            return;
        }
        int poolSize = Math.min(2, redshiftTablesToClone.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("redshift-clone", poolSize);
        List<Future<?>> futures = new ArrayList<>();
        redshiftTablesToClone.forEach((src, tgt) -> {
            Future<?> future = executorService.submit(() -> {
                String msg = "Clone redshift table " + src + " to " + tgt;
                try (PerformanceTimer timer = new PerformanceTimer(msg)) {
                    redshiftService.cloneTable(src, tgt);
                    RedshiftDataUnit dataUnit = new RedshiftDataUnit();
                    dataUnit.setTenant(CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
                    dataUnit.setName(tgt);
                    dataUnit.setRedshiftTable(tgt.toLowerCase());
                    dataUnitProxy.create(mainTestTenant.getId(), dataUnit);
                }
            });
            futures.add(future);
        });
        while (!futures.isEmpty()) {
            List<Future<?>> completed = new ArrayList<>();
            futures.forEach(future -> {
                try {
                    future.get(5, TimeUnit.SECONDS);
                    completed.add(future);
                } catch (TimeoutException e) {
                    // ignore
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException("Failed to clone redshift table.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        executorService.shutdown();
    }

    private void unzipCheckpoint(String checkpoint, String version) throws IOException {
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
    private void verifyStatistics() {
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

    private void uploadCheckpointHdfs(String checkpoint) throws IOException {
        log.info("Start uploading checkpoint " + checkpoint + " to hdfs.");
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data").toString();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localDir, targetPath);
        log.info("Upload checkpoint to hdfs path " + targetPath);
    }

    private DataCollection.Version getCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = String.format("%s/%s/_VERSION_", checkpointDir, checkpoint);
        String version = FileUtils.readFileToString(new File(versionFile), "UTF-8").trim();
        return DataCollection.Version.valueOf(version);
    }

    private DynamoDataUnit parseDynamoDataUnit(String checkpoint, String roleName, DataCollection.Version version)
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

    private List<Table> parseCheckpointTable(String checkpoint, String roleName, DataCollection.Version version,
            String[] tenantNames) throws IOException {
        String jsonFilePath = String.format("%s/%s/%s/tables/%s.json", checkpointDir, checkpoint, version.name(),
                roleName);
        log.info("Checking table json file path " + jsonFilePath);
        File jsonFile = new File(jsonFilePath);
        if (!jsonFile.exists()) {
            return null;
        }

        log.info("Parse check point " + checkpoint + " table " + roleName + " of version " + version.name());
        List<Table> tables = new ArrayList<>();
        ArrayNode arrNode = (ArrayNode) om.readTree(jsonFile);
        Iterator<JsonNode> iter = arrNode.elements();
        while (iter.hasNext()) {
            JsonNode json = iter.next();
            String hdfsPath = json.get("extracts_directory").asText();
            if (StringUtils.isBlank(hdfsPath)) {
                hdfsPath = json.get("extracts").get(0).get("path").asText();
                if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
                    hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                }
            }
            log.info("Parse extract path " + hdfsPath);
            Pattern pattern = Pattern.compile("/Contracts/(.*)/Tenants/");
            Matcher matcher = pattern.matcher(hdfsPath);
            String str = JsonUtils.serialize(json);
            str = str.replaceAll("/Pods/Default/", "/Pods/" + podId + "/");
            str = str.replaceAll("/Pods/QA/", "/Pods/" + podId + "/");
            if (matcher.find()) {
                tenantNames[0] = matcher.group(1);
                log.info("Found tenant name " + tenantNames[0] + " in json.");
            } else {
                log.info("Cannot find tenant for " + tenantNames[0]);
            }

            if (tenantNames[0] != null) {
                String testTenant = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
                String hdfsPathSegment1 = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
                String hdfsPathSegment2 = hdfsPath.substring(hdfsPath.lastIndexOf("/"));
                if (hdfsPathSegment2.contains(tenantNames[0])) {
                    String hdfsPathIntermediatePattern = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) //
                            + "/$$TABLE_DATA_DIR$$";
                    String hdfsPathFinal = hdfsPathSegment1.replaceAll(tenantNames[0], testTenant) + hdfsPathSegment2;
                    str = str.replaceAll(hdfsPath, hdfsPathIntermediatePattern);
                    str = str.replaceAll(tenantNames[0], testTenant);
                    str = str.replaceAll(hdfsPathIntermediatePattern, hdfsPathFinal);
                } else {
                    str = str.replaceAll(tenantNames[0], testTenant);
                }
            }
            tables.add(JsonUtils.deserialize(str, Table.class));
        }

        return tables;
    }

    private StatisticsContainer parseCheckpointStatistics(String checkpoint, DataCollection.Version version)
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

    private DataCollectionStatus parseDataCollectionStatus(String checkpoint, DataCollection.Version version)
            throws IOException {
        String jsonFile = String.format("%s/%s/%s/data_collection_status.json", checkpointDir, checkpoint,
                version.name());
        if (!new File(jsonFile).exists()) {
            return null;
        }
        return JsonUtils.deserialize(new FileInputStream(jsonFile), DataCollectionStatus.class);
    }

    public void cleanup() {
        if (StringUtils.isNotBlank(checkpointDir)) {
            FileUtils.deleteQuietly(new File(checkpointDir));
        }
    }

    private void resumeDbState() {
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

    public void saveCheckpoint(String checkpointName, String checkpointVersion) throws IOException {
        String rootDir = "checkpoints/" + checkpointName;
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpointName);

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = "checkpoints/" + checkpointName + "/" + version.name() + "/tables";
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                }
            }
            saveStatsIfExists(version, checkpointName);
            saveDataCollectionStatus(version, checkpointName);
        }

        printSaveRedshiftStatements(checkpointName, checkpointVersion);
        saveCheckpointVersion(checkpointName);
        printPublishEntityRequest(checkpointName, checkpointVersion);
    }

    public void saveCheckpoint(String checkpointName, String checkpointVersion, String customerSpace)
            throws IOException {
        String rootDir = "checkpoints/" + checkpointName;
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpointName);

        DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = "checkpoints/" + checkpointName + "/" + version.name() + "/tables";
            FileUtils.forceMkdir(new File(tablesDir));
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpointName);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                    saveDynamoTableIfExists(checkpointName, role, version);
                }
            }
            saveStatsIfExists(version, checkpointName);
            saveDataCollectionStatus(version, checkpointName);
        }

        printSaveRedshiftStatements(checkpointName, checkpointVersion);
        saveCheckpointVersion(checkpointName);
        // Save Workflow Execution Context.
        saveWorkflowExecutionContext(checkpointName, customerSpace);
        printPublishEntityRequest(checkpointName, checkpointVersion);
    }

    private void saveCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = "checkpoints/" + checkpoint + "/_VERSION_";
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());
        FileUtils.write(new File(versionFile), version.name(), "UTF-8");
    }

    private void downloadHdfsData(String checkpoint) throws IOException {
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

    private void saveTableIfExists(TableRoleInCollection role, DataCollection.Version version, String checkpoint)
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

    private void saveRedshiftTableIfExists(TableRoleInCollection role, DataCollection.Version version) {
        String tableName = dataCollectionProxy.getTableName(mainTestTenant.getId(), role, version);
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

    private void saveDynamoTableIfExists(String checkpoint, TableRoleInCollection role, DataCollection.Version version)
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

    private void saveStatsIfExists(DataCollection.Version version, String checkpoint) throws IOException {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId(), version);
        if (statisticsContainer != null) {
            String jsonFile = String.format("checkpoints/%s/%s/stats_container.json", checkpoint, version.name());
            om.writeValue(new File(jsonFile), statisticsContainer);
            log.info("Save stats at version " + version + " to " + jsonFile);
        } else {
            log.info("There is no stats at version " + version);
        }
    }

    private void saveDataCollectionStatus(DataCollection.Version version, String checkpoint) throws IOException {
        DataCollectionStatus dataCollectionStatus = dataCollectionProxy
                .getOrCreateDataCollectionStatus(mainTestTenant.getId(), version);
        String jsonFile = String.format("checkpoints/%s/%s/data_collection_status.json", checkpoint, version.name());
        om.writeValue(new File(jsonFile), dataCollectionStatus);
        log.info("Save DataCollection Status at version " + version + " to " + jsonFile);
    }

    private void printSaveRedshiftStatements(String checkpointName, String checkpointVersion) {
        if (MapUtils.isNotEmpty(savedRedshiftTables)) {
            StringBuilder msg = new StringBuilder(
                    "If you are going to save the checkpoint to version " + checkpointVersion);
            msg.append(", you can run following statements in redshift:\n\n");
            List<String> dropTables = new ArrayList<>();
            List<String> renameTables = new ArrayList<>();
            savedRedshiftTables.forEach((role, table) -> {
                String tgtTable = checkpointRedshiftTableName(checkpointName, role, checkpointVersion);
                dropTables.add("drop table if exists " + tgtTable + ";");
                renameTables.add(String.format("alter table %s rename to %s;", table, tgtTable));
            });
            for (String statement : dropTables) {
                msg.append(statement).append("\n");
            }
            for (String statement : renameTables) {
                msg.append(statement).append("\n");
            }
            log.info(msg.toString());
        }
    }

    private String checkpointRedshiftTableName(String checkpoint, TableRoleInCollection role,
            String checkpointVersion) {
        return String.format("cdlend2end_%s_%s_%s", checkpoint, role.name(), checkpointVersion);
    }

    private void saveWorkflowExecutionContext(String checkpoint, String customerSpace) throws IOException {
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

    /**
     * For entity match enabled PA, when saving checkpoint for match lookup/seed
     * table, need to publish all the preceding checkpoints' staging lookup/seed
     * table instead of only current tenant.
     * 
     * Reason is in current tenant's staging table, only entries which are
     * touched in match job exist, which means the staging table doesn't have
     * complete entity universe for the tenant. Although serving table has
     * complete entity universe, serving table doesn't support scan due to
     * lookup performance concern.
     * 
     * @param checkpointName
     * @param checkpointVersion
     */
    @VisibleForTesting
    void printPublishEntityRequest(String checkpointName, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            return;
        }
        try {
            StringBuilder msg = new StringBuilder("\nTo publish Entity Match Seed Table version " + checkpointVersion
                    + " you must run the following HTTP Requests:\n");
            for (BusinessEntity businessEntity : Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact)) {
                msg.append("POST " + matchapiHostPort + "/match/matches/entity/versions\n");
                msg.append("Body:\n");
                BumpVersionRequest request = new BumpVersionRequest();
                String destTenantId = getCheckPointTenantId(checkpointName, checkpointVersion, businessEntity.name());
                Tenant destTenant = new Tenant(CustomerSpace.parse(destTenantId).toString());
                request.setTenant(destTenant);
                request.setEnvironments(Arrays.asList(EntityMatchEnvironment.STAGING, EntityMatchEnvironment.SERVING));
                msg.append(om.writerWithDefaultPrettyPrinter().writeValueAsString(request) + "\n");
            }

            msg.append("POST " + matchapiHostPort + "/match/matches/entity/publish/list\n");
            msg.append("Body:\n");

            List<EntityPublishRequest> requests = new ArrayList<>();
            for (BusinessEntity businessEntity: Arrays.asList(BusinessEntity.Account, BusinessEntity.Contact)) {
                String destTenantId = getCheckPointTenantId(checkpointName, checkpointVersion, businessEntity.name());
                Tenant destTenant = new Tenant(CustomerSpace.parse(destTenantId).toString());
                List<Tenant> srcTenants = new ArrayList<>();
                if (CollectionUtils.isNotEmpty(precedingCheckpoints)) {
                    srcTenants.addAll(precedingCheckpoints.stream()
                            .map(cp -> new Tenant(CustomerSpace
                                    .parse(getCheckPointTenantId(cp, checkpointVersion, businessEntity.name()))
                                    .toString()))
                            .collect(Collectors.toList()));
                }
                srcTenants.add(mainTestTenant);

                for (Tenant srcTenant : srcTenants) {
                    EntityPublishRequest request = new EntityPublishRequest();
                    request.setEntity(businessEntity.name());
                    request.setSrcTenant(srcTenant);

                    request.setDestTenant(destTenant);
                    request.setDestEnv(EntityMatchEnvironment.STAGING);
                    request.setDestTTLEnabled(false);
                    request.setBumpupVersion(false);
                    requests.add(request);

                    request = om.readValue(om.writeValueAsString(request), EntityPublishRequest.class);
                    request.setDestEnv(EntityMatchEnvironment.SERVING);
                    requests.add(request);
                }
            }

            msg.append(om.writerWithDefaultPrettyPrinter().writeValueAsString(requests) + "\n");
            log.info(msg.toString());
        } catch (IOException e) {
            log.error("Failed to print EntityPublishRequest:\n" + e.getMessage(), e);
        }
    }

    private void copyEntitySeedTables(String checkpoint, String checkpointVersion) {
        if (!isEntityMatchEnabled()) {
            return;
        }

        ExecutorService tp = ThreadPoolUtils.getFixedSizeThreadPool("entity-match-copy", 2);
        ThreadPoolUtils.runRunnablesInParallel(tp, Arrays.asList( //
                copyEntitySeedTable(checkpoint, checkpointVersion, BusinessEntity.Account.name()),
                copyEntitySeedTable(checkpoint, checkpointVersion, BusinessEntity.Contact.name())
        ), 10, 1);
        tp.shutdown();
    }

    private Runnable copyEntitySeedTable(String checkpoint, String checkpointVersion, String entity) {
        EntityPublishRequest request = new EntityPublishRequest();
        request.setEntity(entity);
        String srcTenantId = getCheckPointTenantId(checkpoint, checkpointVersion, entity);
        Tenant srcTenant = new Tenant(CustomerSpace.parse(srcTenantId).toString());
        request.setSrcTenant(srcTenant);
        request.setDestTenant(mainTestTenant);
        request.setDestEnv(EntityMatchEnvironment.SERVING);
        request.setDestTTLEnabled(true);
        request.setBumpupVersion(false);
        return () -> {
            log.info("Start copying entity match table for " + entity);
            EntityPublishStatistics stats = matchProxy.publishEntity(request);
            log.info("Copied {} {} seeds and {} {} lookup entries from tenant {} to tenant {}",
                    stats.getSeedCount(), entity, //
                    stats.getLookupCount(), entity, //
                    CustomerSpace.shortenCustomerSpace(srcTenant.getId()), //
                    CustomerSpace.shortenCustomerSpace(mainTestTenant.getId()));
            Assert.assertTrue(stats.getSeedCount() > 0);
            Assert.assertTrue(stats.getLookupCount() > 0);
        };
    }

    public static String getCheckPointTenantId(String checkpoint, String checkpointVersion, String entity) {
        return "cdlend2end_" + checkpoint + "_" + entity.toLowerCase() + "_" + checkpointVersion;
    }

    @VisibleForTesting
    boolean isEntityMatchEnabled() {
        FeatureFlagValueMap flags = batonService.getFeatureFlags(CustomerSpace.parse(mainTestTenant.getId()));
        return FeatureFlagUtils.isEntityMatchEnabled(flags);
    }
}
