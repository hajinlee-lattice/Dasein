package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.etl.purge.entitymgr.PurgeStrategyEntityMgr;
import com.latticeengines.datacloud.etl.service.HiveTableService;
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component
public class PurgeServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {
    public final String POD_ID = this.getClass().getSimpleName();

    private static Logger log = LoggerFactory.getLogger(PurgeServiceImplTestNG.class);

    @Autowired
    private PurgeService purgeService;

    @Autowired
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Autowired
    private HiveTableService hiveTableService;

    @Autowired
    protected DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    private PurgeSource pipelineTempSourceToPurge;
    private PurgeSource operationalSourceToPurge;
    private PurgeSource hiveSourceToPurge;
    private PurgeSource ingestionToPurge;
    private PurgeSource generalSourceToBak;
    private PurgeSource generalSourceToDelete;
    private PurgeSource amToDelete;
    private PurgeSource amToBak;
    private PurgeSource amLookupToDelete;
    private PurgeSource amLookupToBak;
    private PurgeSource mlDailyToDelete;
    private PurgeSource mlDailyToBak;
    private String unknownSource = "TestUnknownSource";

    private Map<String, PurgeSource> validationMapNonDebugMode;
    private Map<String, PurgeSource> validationMapDebugMode;

    private Date now = new Date();

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        preparePipelineTempSource();
        prepareOperationalSourceToPurge();
        prepareHiveSource();
        prepareIngestionToPurge();
        prepareGeneralSourceToPurge();
        prepareAMSourceToPurge();
        prepareMLSourceToPurge();
        prepareUnknownSource();
        prepareValidationMap();
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource("Pipeline_"));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource("LDCDEV_"));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(ingestionToPurge.getSourceName()));
        purgeStrategyEntityMgr
                .delete(purgeStrategyEntityMgr.findStrategiesBySource(generalSourceToBak.getSourceName()));
        purgeStrategyEntityMgr
                .delete(purgeStrategyEntityMgr.findStrategiesBySource(generalSourceToDelete.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(amToDelete.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(amLookupToDelete.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource(mlDailyToDelete.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategiesBySource("Hive"));
    }

    /**
     * Such exception is by design in the scan
     * java.lang.RuntimeException: Failed to get all versions for Pipeline_AccountMasterSeedClean_version_2018-01-10_05-41-36_UTC_step_1
     * Caused by: java.io.FileNotFoundException: File /Pods/PurgeServiceImplTestNG/Services/PropData/Sources/Pipeline_AccountMasterSeedClean_version_2018-01-10_05-41-36_UTC_step_1/Snapshot does not exist.
     */
    @Test(groups = "functional")
    public void testScan() throws IOException {
        List<PurgeSource> toPurge = purgeService.scan(POD_ID, false);
        log.info("Validating purge sources in non-debug mode");
        validatePurgeSources(toPurge, validationMapNonDebugMode);

        toPurge = purgeService.scan(POD_ID, true);
        log.info("Validating purge sources in debug mode");
        validatePurgeSources(toPurge, validationMapDebugMode);
    }

    @Test(groups = "functional")
    public void testScanUnknownSource() {
        List<String> unknownSources = purgeService.scanUnknownSources(POD_ID);
        Assert.assertNotNull(unknownSources);
        Assert.assertTrue(unknownSources.size() >= 1);
        Set<String> unknown = new HashSet<>(unknownSources);
        Assert.assertTrue(unknown.contains(unknownSource));
    }

    private void preparePipelineTempSource() throws IOException {
        String srcName = "Pipeline_AccountMasterSeedClean_version_2018-01-10_05-41-36_UTC_step_1";
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(srcName, "2018-02-25_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPathBuilder.constructSourceDir(srcName).toString());
        pipelineTempSourceToPurge = new PurgeSource(srcName, hdfsPaths, null, false);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource("Pipeline_");
        strategy.setSourceType(SourceType.TEMP_SOURCE);
        strategy.setHdfsDays(3);
        strategy.setNoBak(true);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareOperationalSourceToPurge() throws IOException {
        String srcName = "LDCDEV_SuspectRecords";
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(srcName, "2018-02-25_00-00-00_UTC").toString();

        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPathBuilder.constructSourceDir(srcName).toString());
        String hiveTable = hiveTableService.tableName(srcName, "2018-02-25_00-00-00_UTC");
        List<String> hiveTables = Collections.singletonList(hiveTable);
        operationalSourceToPurge = new PurgeSource(srcName, hdfsPaths, hiveTables, false);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource("LDCDEV_");
        strategy.setSourceType(SourceType.TEMP_SOURCE);
        strategy.setHdfsDays(14);
        strategy.setNoBak(true);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareHiveSource() throws IOException {
        String srcName = "TestHiveToDelete";
        String hdfsPath = new Path("/apps/hive/warehouse", srcName).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath) || HdfsUtils.isDirectory(yarnConfiguration, hdfsPath)) {
            HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
        }
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPath);
        List<String> hiveTables = Collections.singletonList(srcName.toLowerCase());
        hiveSourceToPurge = new PurgeSource(srcName, hdfsPaths, hiveTables, false);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource("Hive");
        strategy.setSourceType(SourceType.HDFS_DIR);
        strategy.setHdfsDays(15);
        strategy.setHdfsBasePath("/apps/hive/warehouse");
        strategy.setNoBak(true);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareIngestionToPurge() throws IOException {
        String ingestionName = "TestIngestionToPurge";
        String hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-25_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-18_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-11_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPath);
        ingestionToPurge = new PurgeSource(ingestionName, hdfsPaths, null, true);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(ingestionName);
        strategy.setSourceType(SourceType.INGESTION_SOURCE);
        strategy.setHdfsVersions(2);
        strategy.setS3Days(100);
        strategy.setGlacierDays(100);
        strategy.setNoBak(false);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareGeneralSourceToPurge() throws IOException {
        String sourceName = "TestGeneralSourceToBak";
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        String schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        List<String> hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        String hiveTable = hiveTableService.tableName(sourceName, "2018-02-11_00-00-00_UTC");
        List<String> hiveTables = Collections.singletonList(hiveTable);
        generalSourceToBak = new PurgeSource(sourceName, hdfsPaths, hiveTables, true);
        PurgeStrategy strategyToBak = new PurgeStrategy();
        strategyToBak.setSource(sourceName);
        strategyToBak.setSourceType(SourceType.GENERAL_SOURCE);
        strategyToBak.setHdfsVersions(2);
        strategyToBak.setS3Days(100);
        strategyToBak.setGlacierDays(100);
        strategyToBak.setNoBak(false);

        sourceName = "TestGeneralSourceToDelete";
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        hiveTable = hiveTableService.tableName(sourceName, "2018-02-11_00-00-00_UTC");
        hiveTables = Collections.singletonList(hiveTable);
        generalSourceToDelete = new PurgeSource(sourceName, hdfsPaths, hiveTables, false);
        PurgeStrategy strategyToDelete = new PurgeStrategy();
        strategyToDelete.setSource(sourceName);
        strategyToDelete.setSourceType(SourceType.GENERAL_SOURCE);
        strategyToDelete.setHdfsDays(90);
        strategyToDelete.setS3Days(100);
        strategyToDelete.setGlacierDays(100);
        strategyToDelete.setNoBak(true);

        purgeStrategyEntityMgr.insertAll(Arrays.asList(strategyToBak, strategyToDelete));
    }

    private void prepareAMSourceToPurge() throws IOException {
        String sourceName = AMSourcePurger.ACCOUNT_MASTER;
        String versionToRetain1 = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountMasterHdfsVersion();
        String versionToRetain2 = HdfsPathBuilder.dateFormat.format(new Date());
        String versionToBak = dataCloudVersionEntityMgr.findVersion("2.0.6")
                .getAccountMasterHdfsVersion();
        String versionToDelete = "2000-01-01_00-00-00_UTC";

        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain1).toString();
        String schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain1).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain2).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain2).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToBak).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToBak).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        List<String> hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        String hiveTable = hiveTableService.tableName(sourceName, versionToBak);
        List<String> hiveTables = Collections.singletonList(hiveTable);
        amToBak = new PurgeSource(sourceName, hdfsPaths, hiveTables, true);

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToDelete).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToDelete).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        hiveTable = hiveTableService.tableName(sourceName, versionToDelete);
        hiveTables = Collections.singletonList(hiveTable);
        amToDelete = new PurgeSource(sourceName, hdfsPaths, hiveTables, false);

        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.AM_SOURCE);
        strategy.setHdfsVersions(2);
        strategy.setS3Days(30);
        strategy.setGlacierDays(1170);
        strategy.setNoBak(false);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));

        sourceName = AMSourcePurger.ACCOUNT_MASTER_LOOKUP;
        versionToRetain1 = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountLookupHdfsVersion();
        versionToRetain2 = HdfsPathBuilder.dateFormat.format(new Date());
        versionToBak = dataCloudVersionEntityMgr.findVersion("2.0.6").getAccountLookupHdfsVersion();
        versionToDelete = "2000-01-01_00-00-00_UTC";

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain1).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain1).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain2).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain2).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToBak).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToBak).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        hiveTable = hiveTableService.tableName(sourceName, versionToBak);
        hiveTables = Collections.singletonList(hiveTable);
        amLookupToBak = new PurgeSource(sourceName, hdfsPaths, hiveTables, true);

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToDelete).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToDelete).toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        HdfsUtils.mkdir(yarnConfiguration, schemaPath);
        hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        hiveTable = hiveTableService.tableName(sourceName, versionToDelete);
        hiveTables = Collections.singletonList(hiveTable);
        amLookupToDelete = new PurgeSource(sourceName, hdfsPaths, hiveTables, false);

        strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.AM_SOURCE);
        strategy.setHdfsVersions(2);
        strategy.setS3Days(30);
        strategy.setGlacierDays(1170);
        strategy.setNoBak(false);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareMLSourceToPurge() throws IOException {
        String sourceName = "TestMadisonLogic";
        String versionFormat = "yyyy-MM-dd";
        String hdfsBasePath = "/user/propdata/madison/dataflow/incremental";
        DateFormat df = new SimpleDateFormat(versionFormat);
        String versionToRetain = df.format(now);
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -250);
        String versionToBak = df.format(cal.getTime());
        cal.add(Calendar.DATE, -250);
        String versionToDelete = df.format(cal.getTime());

        String hdfsPath = new Path(hdfsBasePath, versionToRetain).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        }

        hdfsPath = new Path(hdfsBasePath, versionToBak).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        }
        List<String> hdfsPaths = Arrays.asList(hdfsPath);
        mlDailyToBak = new PurgeSource(sourceName, hdfsPaths, null, true);

        hdfsPath = new Path(hdfsBasePath, versionToDelete).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        }
        hdfsPaths = Arrays.asList(hdfsPath);
        mlDailyToDelete = new PurgeSource(sourceName, hdfsPaths, null, false);

        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.TIMESERIES_SOURCE);
        strategy.setHdfsBasePath(hdfsBasePath);
        strategy.setVersionFormat(versionFormat);
        strategy.setHdfsDays(200);
        strategy.setS3Days(30);
        strategy.setGlacierDays(170);
        strategy.setNoBak(false);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareUnknownSource() throws IOException {
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(unknownSource, "2018-02-25_00-00-00_UTC").toString();
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
    }

    private void prepareValidationMap() {
        validationMapNonDebugMode = new HashMap<>();
        validationMapNonDebugMode.put(getValidationKey(ingestionToPurge), ingestionToPurge);
        validationMapNonDebugMode.put(getValidationKey(generalSourceToBak), generalSourceToBak);
        validationMapNonDebugMode.put(getValidationKey(amToBak), amToBak);
        validationMapNonDebugMode.put(getValidationKey(amToDelete), amToDelete);
        validationMapNonDebugMode.put(getValidationKey(amLookupToBak), amLookupToBak);
        validationMapNonDebugMode.put(getValidationKey(amLookupToDelete), amLookupToDelete);
        validationMapNonDebugMode.put(getValidationKey(mlDailyToBak), mlDailyToBak);
        validationMapNonDebugMode.put(getValidationKey(mlDailyToDelete), mlDailyToDelete);

        validationMapDebugMode = new HashMap<>();
        validationMapDebugMode.put(getValidationKey(pipelineTempSourceToPurge), pipelineTempSourceToPurge);
        validationMapDebugMode.put(getValidationKey(operationalSourceToPurge), operationalSourceToPurge);
        validationMapDebugMode.put(getValidationKey(hiveSourceToPurge), hiveSourceToPurge);
        validationMapDebugMode.put(getValidationKey(ingestionToPurge), ingestionToPurge);
        validationMapDebugMode.put(getValidationKey(generalSourceToBak), generalSourceToBak);
        validationMapDebugMode.put(getValidationKey(amToBak), amToBak);
        validationMapDebugMode.put(getValidationKey(amToDelete), amToDelete);
        validationMapDebugMode.put(getValidationKey(amLookupToBak), amLookupToBak);
        validationMapDebugMode.put(getValidationKey(amLookupToDelete), amLookupToDelete);
        validationMapDebugMode.put(getValidationKey(mlDailyToBak), mlDailyToBak);
        validationMapDebugMode.put(getValidationKey(mlDailyToDelete), mlDailyToDelete);

        // generalSourceToDelete should not be any of these map because upload
        // days is not old enough based on HdfsDays
    }

    private String getValidationKey(PurgeSource purgeSource) {
        return purgeSource.getSourceName() + "_ToBak_" + purgeSource.isToBak();
    }

    private void validatePurgeSources(List<PurgeSource> toPurge, Map<String, PurgeSource> validationMap) {
        Assert.assertTrue(CollectionUtils.isNotEmpty(toPurge));
        Set<String> actualKeys = new HashSet<>();
        toPurge.forEach(purgeSource -> {
            String key = getValidationKey(purgeSource);
            log.info("Validating " + JsonUtils.serialize(purgeSource));
            PurgeSource expected = validationMap.get(key);
            Assert.assertNotNull(expected);
            log.info("Expecting " + JsonUtils.serialize(expected));
            Assert.assertEquals(purgeSource.isToBak(), expected.isToBak());
            if (purgeSource.getSourceName().equals(mlDailyToBak.getSourceName())) {
                validateMLSource(purgeSource);
            } else {
                Assert.assertTrue(isIdenticalList(expected.getHdfsPaths(), purgeSource.getHdfsPaths()));
                Assert.assertTrue(
                        isIdenticalList(expected.getHiveTables(), purgeSource.getHiveTables()));
            }
            if (validationMap.containsKey(key)) {
                actualKeys.add(key);
            }
        });
        Assert.assertEquals(actualKeys.size(), validationMap.size());
    }

    private boolean isIdenticalList(List<String> expected, List<String> actual) {
        if (CollectionUtils.isEmpty(expected) && CollectionUtils.isEmpty(actual)) {
            return true;
        }
        if (CollectionUtils.isEmpty(expected) || CollectionUtils.isEmpty(actual)) {
            return false;
        }
        if (expected.size() != actual.size()) {
            return false;
        }
        Set<String> expectedSet = new HashSet<>(expected);
        for (String ent : actual) {
            if (!expectedSet.contains(ent)) {
                return false;
            }
        }
        return true;
    }

    private void validateMLSource(PurgeSource purgeSource) {
        Assert.assertTrue(CollectionUtils.isNotEmpty(purgeSource.getHdfsPaths()));
        Set<String> hdfsPathSet = new HashSet<>(purgeSource.getHdfsPaths());
        if (purgeSource.isToBak()) {
            Assert.assertTrue(hdfsPathSet.contains(mlDailyToBak.getHdfsPaths().get(0)));
            Assert.assertFalse(hdfsPathSet.contains(mlDailyToDelete.getHdfsPaths().get(0)));
        } else {
            Assert.assertTrue(hdfsPathSet.contains(mlDailyToDelete.getHdfsPaths().get(0)));
            Assert.assertFalse(hdfsPathSet.contains(mlDailyToBak.getHdfsPaths().get(0)));
        }
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String versionToRetain = df.format(now);
        String hdfsPathToRetain = new Path("/user/propdata/madison/dataflow/incremental", versionToRetain).toString();
        Assert.assertFalse(hdfsPathSet.contains(hdfsPathToRetain));
    }
}
