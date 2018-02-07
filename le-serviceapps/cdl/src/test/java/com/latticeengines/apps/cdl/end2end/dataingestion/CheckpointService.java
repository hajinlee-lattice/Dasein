package com.latticeengines.apps.cdl.end2end.dataingestion;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.statistics.AttributeStats;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.datacloud.statistics.Buckets;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.StatisticsContainer;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedExecution;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.redshiftdb.exposed.service.RedshiftService;
import com.latticeengines.testframework.exposed.service.TestArtifactService;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;

@Component("checkpointService")
public class CheckpointService {

    private static final Logger logger = LoggerFactory.getLogger(CheckpointService.class);

    private static final String S3_CHECKPOINTS_DIR = "le-serviceapps/cdl/end2end/checkpoints";
    private static final String S3_CHECKPOINTS_VERSION = "11";
    static final String CHECKPOINT_DATASET_VDB = "vdb";
    static final String CHECKPOINT_DATASET_CSV = "csv";

    static final int ACCOUNT_IMPORT_SIZE_1 = 500;
    static final int ACCOUNT_IMPORT_SIZE_2 = 300;

    static final int CONTACT_IMPORT_SIZE_1 = 1100;
    static final int CONTACT_IMPORT_SIZE_2 = 1200;

    static final int PRODUCT_IMPORT_SIZE_1 = 100;
    static final int PRODUCT_IMPORT_SIZE_2 = 49;

    static final int TRANSACTION_IMPORT_SIZE_1 = 30000;
    static final int TRANSACTION_IMPORT_SIZE_2 = 30000;

    private static final int NUM_PURCHASE_HISTORY_1 = 500;
    private static final int NUM_PURCHASE_HISTORY_2 = 800;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    @Inject
    private DataFeedProxy dataFeedProxy;

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

    @Resource(name = "jdbcTemplate")
    private JdbcTemplate jdbcTemplate;

    @Value("${camille.zk.pod.id}")
    private String podId;

    @Value("${common.le.stack}")
    private String leStack;

    private ObjectMapper om = new ObjectMapper();

    private Tenant mainTestTenant;

    private String checkpointDir;

    private Map<TableRoleInCollection, String> savedRedshiftTables =  new HashMap<>();

    void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

    void verifyFirstProfileCheckpoint() throws IOException {
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of(TableRoleInCollection.ConsolidatedAccount,
                (long) ACCOUNT_IMPORT_SIZE_1, TableRoleInCollection.ConsolidatedContact, (long) CONTACT_IMPORT_SIZE_1,
                TableRoleInCollection.ConsolidatedProduct, (long) PRODUCT_IMPORT_SIZE_1,
                TableRoleInCollection.CalculatedPurchaseHistory, (long) NUM_PURCHASE_HISTORY_1);
        verifyCheckpoint(expectedCounts);
    }

    void verifySecondConsolidateCheckpoint() throws IOException {
        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        long numProducts = PRODUCT_IMPORT_SIZE_1 + PRODUCT_IMPORT_SIZE_2;
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of(TableRoleInCollection.ConsolidatedAccount,
                numAccounts, TableRoleInCollection.ConsolidatedContact, numContacts,
                TableRoleInCollection.ConsolidatedProduct, numProducts);
        verifyCheckpoint(expectedCounts);
    }

    void verifySecondProfileCheckpoint() throws IOException {
        long numAccounts = ACCOUNT_IMPORT_SIZE_1 + ACCOUNT_IMPORT_SIZE_2;
        long numContacts = CONTACT_IMPORT_SIZE_1 + CONTACT_IMPORT_SIZE_2;
        long numProducts = PRODUCT_IMPORT_SIZE_1 + PRODUCT_IMPORT_SIZE_2;
        Map<TableRoleInCollection, Long> expectedCounts = ImmutableMap.of(TableRoleInCollection.ConsolidatedAccount,
                numAccounts, TableRoleInCollection.ConsolidatedContact, numContacts,
                TableRoleInCollection.ConsolidatedProduct, numProducts, TableRoleInCollection.CalculatedPurchaseHistory,
                (long) NUM_PURCHASE_HISTORY_2);
        verifyCheckpoint(expectedCounts);
    }

    private void verifyCheckpoint(Map<TableRoleInCollection, Long> expectedCounts) throws IOException {
        verifyHdfsCheckpoint();
        DataFeed dataFeed = dataFeedProxy.getDataFeed(mainTestTenant.getId());
        Assert.assertEquals(DataFeed.Status.Active, dataFeed.getStatus());
        Assert.assertNotNull(dataFeed.getActiveExecution(), "Should have active execution.");
        Assert.assertNotNull(dataFeed.getActiveProfile(), "Should have active profile.");

        verifyStatistics();

        expectedCounts.forEach((role, count) -> Assert.assertEquals(countTableRole(role), count.longValue()));
    }

    void resumeCheckpoint(String checkpoint, String dataset) throws IOException {
        unzipCheckpoint(checkpoint);

        dataFeedProxy.getDataFeed(mainTestTenant.getId());
        String[] tenantNames = new String[1];

        DataCollection.Version activeVersion = getCheckpointVersion(checkpoint);

        Map<String, String> redshiftTablesToClone = new HashMap<>();
        for (DataCollection.Version version : DataCollection.Version.values()) {
            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                Table table = parseCheckpointTable(checkpoint, role.name(), version, tenantNames);
                if (table != null) {
                    logger.info("Creating table " + table.getName() + " for " + role + " in version " + version);
                    metadataProxy.createTable(mainTestTenant.getId(), table.getName(), table);
                    dataCollectionProxy.upsertTable(mainTestTenant.getId(), table.getName(), role, version);
                    if (activeVersion.equals(version)) {
                        String redshiftTable = checkpointRedshiftTableName(checkpoint, role, S3_CHECKPOINTS_VERSION);
                        if (redshiftService.hasTable(redshiftTable)) {
                            redshiftTablesToClone.put(redshiftTable, table.getName());
                        }
                    }
                }
            }
            StatisticsContainer statisticsContainer = parseCheckpointStatistics(checkpoint, version);
            if (statisticsContainer != null) {
                dataCollectionProxy.upsertStats(mainTestTenant.getId(), statisticsContainer);
            }
        }

        cloneRedshiftTables(redshiftTablesToClone);

        uploadCheckpointHdfs(checkpoint);

        dataFeedProxy.updateDataFeedStatus(mainTestTenant.getId(), DataFeed.Status.Active.name());
        resumeDbState();

        dataCollectionProxy.switchVersion(mainTestTenant.getId(), activeVersion);
        logger.info("Switch active version to " + activeVersion);
    }

    private void cloneRedshiftTables(Map<String, String> redshiftTablesToClone) {
        if (MapUtils.isEmpty(redshiftTablesToClone)) {
            return;
        }
        int poolSize = Math.min(2, redshiftTablesToClone.size());
        ExecutorService executorService = ThreadPoolUtils.getFixedSizeThreadPool("redshift-clone", poolSize);
        List<Future<?>> futures = new ArrayList<>();
        redshiftTablesToClone.forEach((src, tgt) -> {
            Future future = executorService.submit(() -> {
                String msg = "Clone redshift table " + src + " to " + tgt;
                try (PerformanceTimer timer = new PerformanceTimer(msg)) {
                    redshiftService.cloneTable(src, tgt);
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
                } catch (InterruptedException|ExecutionException e) {
                    throw new RuntimeException("Failed to clone redshift table.", e);
                }
            });
            completed.forEach(futures::remove);
        }
        executorService.shutdown();
    }

    private void unzipCheckpoint(String checkpoint) throws IOException {
        checkpointDir = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        File downloadedFile = testArtifactService.downloadTestArtifact(S3_CHECKPOINTS_DIR, S3_CHECKPOINTS_VERSION,
                checkpoint + ".zip");
        String zipFilePath = downloadedFile.getPath();
        try {
            ZipFile zipFile = new ZipFile(zipFilePath);
            zipFile.extractAll(checkpointDir);
        } catch (ZipException e) {
            throw new IOException("Failed to unzip checkpoint archive " + zipFilePath, e);
        }
        logger.info(String.format("Unzip checkpoint archive %s to local dir %s", zipFilePath, checkpointDir));
    }

    long countTableRole(TableRoleInCollection role) {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        Table table = dataCollectionProxy.getTable(customerSpace.toString(), role);
        if (table == null) {
            Assert.fail("Cannot find table in role " + role);
        }
        logger.info("countRole " + role.name() + " Table " + table.getName() + " Path "
                + table.getExtracts().get(0).getPath());
        String path = table.getExtracts().get(0).getPath();
        if (!path.endsWith(".avro")) {
            path += path.endsWith("/") ? "*.avro" : "/*.avro";
        }
        return AvroUtils.count(yarnConfiguration, path);
    }

    long countInRedshift(BusinessEntity entity) {
        FrontEndQuery frontEndQuery = new FrontEndQuery();
        frontEndQuery.setMainEntity(entity);
        return entityProxy.getCount(mainTestTenant.getId(), frontEndQuery);
    }

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

    private void verifyHdfsCheckpoint() throws IOException {
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        Path dataRoot = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data");
        HdfsUtils.fileExists(yarnConfiguration, dataRoot.toString());
        List<String> tables = HdfsUtils.getFilesForDir(yarnConfiguration, dataRoot.append("Tables").toString());
        Assert.assertFalse(tables.isEmpty());
        for (String tableHdfsPath : tables) {
            String tableName = tableHdfsPath.substring(tableHdfsPath.lastIndexOf("/") + 1);
            if (!"VisiDB".equals(tableName) && !tableName.contains("copy")) {
                logger.info("Checking table " + tableName);
                if (tableName.startsWith("AnalyticPurchaseState")) {
                    continue;
                }
                Iterator<GenericRecord> iterator = AvroUtils.iterator(yarnConfiguration, tableHdfsPath + "/*.avro");
                Assert.assertTrue(iterator.hasNext(), "Table at " + tableHdfsPath + " does not have avro record.");
            }
        }
    }

    private void uploadCheckpointHdfs(String checkpoint) throws IOException {
        logger.info("Start uploading checkpoint " + checkpoint + " to hdfs.");
        String localDir = checkpointDir + "/" + checkpoint + "/hdfs/Data";
        CustomerSpace cs = CustomerSpace.parse(mainTestTenant.getId());
        String targetPath = PathBuilder.buildCustomerSpacePath(podId, cs).append("Data").toString();
        HdfsUtils.copyFromLocalDirToHdfs(yarnConfiguration, localDir, targetPath);
        logger.info("Upload checkpoint to hdfs path " + targetPath);
    }

    private DataCollection.Version getCheckpointVersion(String checkpoint) throws IOException {
        String versionFile = String.format("%s/%s/_VERSION_", checkpointDir, checkpoint);
        String version = FileUtils.readFileToString(new File(versionFile), "UTF-8").trim();
        return DataCollection.Version.valueOf(version);
    }

    private Table parseCheckpointTable(String checkpoint, String roleName, DataCollection.Version version,
            String[] tenantNames) throws IOException {
        String jsonFilePath = String.format("%s/%s/%s/tables/%s.json", checkpointDir, checkpoint, version.name(),
                roleName);
        logger.info("Checking table json file path " + jsonFilePath);
        File jsonFile = new File(jsonFilePath);
        if (!jsonFile.exists()) {
            return null;
        }

        logger.info("Parse check point " + checkpoint + " table " + roleName + " of version " + version.name());
        JsonNode json = om.readTree(jsonFile);
        String hdfsPath = json.get("extracts_directory").asText();
        if (StringUtils.isBlank(hdfsPath)) {
            hdfsPath = json.get("extracts").get(0).get("path").asText();
            if (hdfsPath.endsWith(".avro") || hdfsPath.endsWith("/")) {
                hdfsPath = hdfsPath.substring(0, hdfsPath.lastIndexOf("/"));
            }
        }
        logger.info("Parse extract path " + hdfsPath);
        Pattern pattern = Pattern.compile("/Contracts/(.*)/Tenants/");
        Matcher matcher = pattern.matcher(hdfsPath);
        String str = JsonUtils.serialize(json);
        str = str.replaceAll("/Pods/Default/", "/Pods/" + podId + "/");
        if (matcher.find()) {
            tenantNames[0] = matcher.group(1);
            logger.info("Found tenant name " + tenantNames[0] + " in json.");
        } else {
            logger.info("Cannot find tenant for " + tenantNames[0]);
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

        return JsonUtils.deserialize(str, Table.class);
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

    void cleanup() {
        if (StringUtils.isNotBlank(checkpointDir)) {
            FileUtils.deleteQuietly(new File(checkpointDir));
        }
    }

    private void resumeDbState() {
        logger.info("Resuming DB state.");
        Long tenantPid = getTenantPid();
        Long feedPid = getDataFeedPid(tenantPid);
        Long workflowId = createFakeWorkflow(tenantPid);
        Long executionId = createExecution(feedPid, workflowId);
        updateActiveExecution(feedPid, executionId);

        Long workflowId2 = createFakeWorkflow(tenantPid);
        Long profileId = creatProfile(feedPid, executionId, workflowId2);
        updateActiveProfile(feedPid, profileId);
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
        logger.info("Created a fake workflow " + pid);
        return pid;
    }

    private Long createExecution(long feedPid, long workflowPid) {
        String sql = "INSERT INTO `DATAFEED_EXECUTION` ";
        sql += "(`FK_FEED_ID`, `WORKFLOW_ID`, `STATUS`) VALUES ";
        sql += String.format("(%d, %d, '%s')", feedPid, workflowPid, DataFeedExecution.Status.ProcessAnalyzed.name());
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `DATAFEED_EXECUTION` WHERE `FK_FEED_ID` = " + feedPid;
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        logger.info("Created a fake execution " + pid);
        return pid;
    }

    private Long creatProfile(long feedPid, long execId, long workflowPid) {
        String sql = "INSERT INTO `DATAFEED_PROFILE` ";
        sql += "(`FEED_EXEC_ID`, `WORKFLOW_ID`, `FK_FEED_ID`) VALUES ";
        sql += String.format("(%d, %d, %d)", execId, workflowPid, feedPid);
        jdbcTemplate.execute(sql);

        sql = "SELECT `PID` FROM `DATAFEED_PROFILE` WHERE `FK_FEED_ID` = " + feedPid;
        long pid = jdbcTemplate.queryForObject(sql, Long.class);
        logger.info("Created a fake profile " + pid);
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
        logger.info("Set execution " + executionId + " as active execution of data feed " + feedPid);
    }

    private void updateActiveProfile(Long feedPid, Long profileId) {
        String sql = "UPDATE `DATAFEED` ";
        sql += String.format("SET `ACTIVE_PROFILE` = %d ", profileId);
        sql += String.format("WHERE `PID` = %d", feedPid);
        jdbcTemplate.execute(sql);
        logger.info("Set profile " + profileId + " as active profile of data feed " + feedPid);
    }

    void saveCheckPoint(String checkpoint) throws IOException {
        String rootDir = "checkpoints/" + checkpoint;
        FileUtils.deleteQuietly(new File(rootDir));
        FileUtils.forceMkdirParent(new File(rootDir));

        downloadHdfsData(checkpoint);

        for (DataCollection.Version version : DataCollection.Version.values()) {
            String tablesDir = "checkpoints/" + checkpoint + "/" + version.name() + "/tables";
            FileUtils.forceMkdir(new File(tablesDir));

            DataCollection.Version active = dataCollectionProxy.getActiveVersion(mainTestTenant.getId());

            for (TableRoleInCollection role : TableRoleInCollection.values()) {
                saveTableIfExists(role, version, checkpoint);
                if (active.equals(version)) {
                    saveRedshiftTableIfExists(role, version);
                }
            }

            saveStatsIfExists(version, checkpoint);
        }

        printSaveRedshiftStatements(checkpoint);

        saveCheckpointVersion(checkpoint);
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
        logger.info("Downloaded hdfs path " + targetPath + " to " + localDir);
        Collection<File> crcFiles = FileUtils.listFiles(new File(localDir), new String[] { "crc" }, true);
        crcFiles.forEach(FileUtils::deleteQuietly);
    }

    private void saveTableIfExists(TableRoleInCollection role, DataCollection.Version version, String checkpoint)
            throws IOException {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), role, version);
        if (table != null) {
            String jsonFile = String.format("checkpoints/%s/%s/tables/%s.json", checkpoint, version.name(),
                    role.name());
            om.writeValue(new File(jsonFile), table);
            logger.info("Save " + role + " at version " + version + " to " + jsonFile);
        } else {
            logger.info("There is no " + role + " table at version " + version);
        }
    }

    private void saveRedshiftTableIfExists(TableRoleInCollection role, DataCollection.Version version) {
        Table table = dataCollectionProxy.getTable(mainTestTenant.getId(), role, version);
        if (table != null) {
            List<String> redshiftTables = redshiftService.getTables(table.getName());
            if (CollectionUtils.isNotEmpty(redshiftTables)) {
                if (redshiftTables.size() != 1) {
                    throw new IllegalStateException("There are " + redshiftTables.size()
                            + " redshift tables prefixed by " + table.getName() + ": " + redshiftTables);
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

    private void saveStatsIfExists(DataCollection.Version version, String checkpoint) throws IOException {
        StatisticsContainer statisticsContainer = dataCollectionProxy.getStats(mainTestTenant.getId(), version);
        if (statisticsContainer != null) {
            String jsonFile = String.format("checkpoints/%s/%s/stats_container.json", checkpoint, version.name());
            om.writeValue(new File(jsonFile), statisticsContainer);
            logger.info("Save stats at version " + version + " to " + jsonFile);
        } else {
            logger.info("There is no stats at version " + version);
        }
    }

    private void printSaveRedshiftStatements(String checkpoint) {
        if (MapUtils.isNotEmpty(savedRedshiftTables)) {
            String nextVersion = String.valueOf(Integer.valueOf(S3_CHECKPOINTS_VERSION) + 1);
            String msg = "If you are going to save the checkpoint to version " + nextVersion;
            msg += ", you can run following statements in redshift:\n\n";
            List<String> dropTables = new ArrayList<>();
            List<String> renameTables = new ArrayList<>();
            savedRedshiftTables.forEach((role, table) -> {
                String tgtTable = checkpointRedshiftTableName(checkpoint, role, nextVersion);
                dropTables.add("drop table if exists " + tgtTable + ";");
                renameTables.add(String.format("alter table %s rename to %s;", table, tgtTable));
            });
            for (String statement : dropTables) {
                msg += statement + "\n";
            }
            for (String statement : renameTables) {
                msg += statement + "\n";
            }
            logger.info(msg);
        }
    }

    private String checkpointRedshiftTableName(String checkpoint, TableRoleInCollection role, String checkpointVersion) {
        return String.format("cdlend2end_%s_%s_%s", checkpoint, role.name(), checkpointVersion);
    }

}
