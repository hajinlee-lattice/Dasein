package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.datacloud.core.entitymgr.DataCloudVersionEntityMgr;
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
    private PurgeSource ingestionToPurge;
    private PurgeSource generalSourceToPurge;
    private PurgeSource amToDelete;
    private PurgeSource amToBak;
    private PurgeSource amLookupToDelete;
    private PurgeSource amLookupToBak;

    private Map<String, PurgeSource> validationMapNonDebugMode;
    private Map<String, PurgeSource> validationMapDebugMode;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        preparePipelineTempSource();
        prepareOperationalSourceToPurge();
        prepareIngestionToPurge();
        prepareGeneralSourceToPurge();
        prepareAMSourceToPurge();
        prepareValidationMap();
    }

    @BeforeClass(groups = "functional")
    public void destroy() {
        purgeStrategyEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    public void testScan() throws IOException {
        List<PurgeSource> toPurge = purgeService.scan(POD_ID, false);
        validatePurgeSourcesNonDebugMode(toPurge);

        toPurge = purgeService.scan(POD_ID, true);
        validatePurgeSourcesDebugMode(toPurge);
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
        strategy.setHdfsVersions(7);
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
        strategy.setHdfsVersions(14);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareIngestionToPurge() throws IOException {
        String ingestionName = "IngestionToPurge";
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
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareGeneralSourceToPurge() throws IOException {
        String sourceName = "TestGeneralSource";
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
        generalSourceToPurge = new PurgeSource(sourceName, hdfsPaths, hiveTables, true);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.GENERAL_SOURCE);
        strategy.setHdfsVersions(2);
        strategy.setS3Days(100);
        strategy.setGlacierDays(100);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareAMSourceToPurge() throws IOException {
        String sourceName = "TestAccountMaster";
        String versionToRetain = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountMasterHdfsVersion();
        String versionToBak = dataCloudVersionEntityMgr.findVersion("2.0.0").getAccountMasterHdfsVersion();
        String versionToDelete = "2000-01-01_00-00-00_UTC";

        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain).toString();
        String schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain).toString();
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
        strategy.setSourceType(SourceType.ACCOUNT_MASTER);
        strategy.setHdfsVersions(3);
        strategy.setS3Days(100);
        strategy.setGlacierDays(100);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));

        sourceName = "TestAccountMasterLookup";
        versionToRetain = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountLookupHdfsVersion();
        versionToBak = dataCloudVersionEntityMgr.findVersion("2.0.0").getAccountLookupHdfsVersion();
        versionToDelete = "2000-01-01_00-00-00_UTC";

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, versionToRetain).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, versionToRetain).toString();
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
        strategy.setSourceType(SourceType.ACCOUNT_MASTER_LOOKUP);
        strategy.setHdfsVersions(3);
        strategy.setS3Days(100);
        strategy.setGlacierDays(100);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareValidationMap() {
        validationMapNonDebugMode = new HashMap<>();
        validationMapNonDebugMode.put(getValidationKey(ingestionToPurge), ingestionToPurge);
        validationMapNonDebugMode.put(getValidationKey(generalSourceToPurge), generalSourceToPurge);
        validationMapNonDebugMode.put(getValidationKey(amToBak), amToBak);
        validationMapNonDebugMode.put(getValidationKey(amLookupToBak), amLookupToBak);

        validationMapDebugMode = new HashMap<>();
        validationMapDebugMode.put(getValidationKey(pipelineTempSourceToPurge), pipelineTempSourceToPurge);
        validationMapDebugMode.put(getValidationKey(operationalSourceToPurge), operationalSourceToPurge);
        validationMapDebugMode.put(getValidationKey(ingestionToPurge), ingestionToPurge);
        validationMapDebugMode.put(getValidationKey(generalSourceToPurge), generalSourceToPurge);
        validationMapDebugMode.put(getValidationKey(amToBak), amToBak);
        validationMapDebugMode.put(getValidationKey(amToDelete), amToDelete);
        validationMapDebugMode.put(getValidationKey(amLookupToBak), amLookupToBak);
        validationMapDebugMode.put(getValidationKey(amLookupToDelete), amLookupToDelete);
    }

    private String getValidationKey(PurgeSource purgeSource) {
        return purgeSource.getSourceName() + "_ToBak_" + purgeSource.isToBak();
    }

    private void validatePurgeSourcesNonDebugMode(List<PurgeSource> toPurge) {
        Assert.assertEquals(toPurge.size(), 4);
        toPurge.forEach(purgeSource -> {
            log.info("Validating " + JsonUtils.serialize(purgeSource));
            PurgeSource expected = validationMapNonDebugMode.get(getValidationKey(purgeSource));
            Assert.assertNotNull(expected);
            log.info("Expecting " + JsonUtils.serialize(expected));
            Assert.assertTrue(isValidatedList(expected.getHdfsPaths(), purgeSource.getHdfsPaths()));
            Assert.assertTrue(isValidatedList(expected.getHiveTables(), purgeSource.getHiveTables()));
            Assert.assertEquals(purgeSource.isToBak(), expected.isToBak());
        });
    }

    private void validatePurgeSourcesDebugMode(List<PurgeSource> toPurge) {
        Assert.assertEquals(toPurge.size(), 8);
        toPurge.forEach(purgeSource -> {
            log.info("Validating " + JsonUtils.serialize(purgeSource));
            PurgeSource expected = validationMapDebugMode.get(getValidationKey(purgeSource));
            Assert.assertNotNull(expected);
            log.info("Expecting " + JsonUtils.serialize(expected));
            Assert.assertTrue(isValidatedList(expected.getHdfsPaths(), purgeSource.getHdfsPaths()));
            Assert.assertTrue(isValidatedList(expected.getHiveTables(), purgeSource.getHiveTables()));
            Assert.assertEquals(purgeSource.isToBak(), expected.isToBak());
        });
    }

    private boolean isValidatedList(List<String> expected, List<String> actual) {
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
}
