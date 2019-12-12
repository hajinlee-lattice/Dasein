package com.latticeengines.datacloudapi.engine.purge.service.impl;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import com.latticeengines.datacloudapi.engine.purge.service.PurgeService;
import com.latticeengines.datacloudapi.engine.testframework.PropDataEngineFunctionalTestNGBase;
import com.latticeengines.domain.exposed.datacloud.DataCloudConstants;
import com.latticeengines.domain.exposed.datacloud.manage.DataCloudVersion;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeSource;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy;
import com.latticeengines.domain.exposed.datacloud.manage.PurgeStrategy.SourceType;

@Component
public class PurgeServiceImplTestNG extends PropDataEngineFunctionalTestNGBase {
    public final String POD_ID = this.getClass().getSimpleName();

    private static Logger log = LoggerFactory.getLogger(PurgeServiceImplTestNG.class);

    @Inject
    private PurgeService purgeService;

    @Inject
    private PurgeStrategyEntityMgr purgeStrategyEntityMgr;

    @Inject
    protected DataCloudVersionEntityMgr dataCloudVersionEntityMgr;

    private PurgeSource pipelineTempSource;
    private PurgeSource operationalSource;
    private PurgeSource ingestionSource;
    private PurgeSource generalSource;
    private PurgeSource am;
    private PurgeSource amLookup;
    private PurgeSource dunsGuideBook;
    private PurgeSource mlDaily;
    private PurgeSource matchResult;
    private String unknownSource = "TestUnknownSource";

    private Map<String, PurgeSource> validationMapNonDebugMode;
    private Map<String, PurgeSource> validationMapDebugMode;

    private List<String> hdfsPathsCleanup = new ArrayList<>();

    private Date now = new Date();

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        prepareCleanPod(POD_ID);
        preparePipelineTempSource();
        prepareOperationalSource();
        prepareIngestion();
        prepareGeneralSource();
        prepareAMSource(DataCloudConstants.ACCOUNT_MASTER);
        prepareAMSource(DataCloudConstants.ACCOUNT_MASTER_LOOKUP);
        prepareAMSource(DataCloudConstants.DUNS_GUIDE_BOOK);
        prepareMLSource();
        prepareMatchResult();
        prepareUnknownSource();
        prepareValidationMap();
    }

    @AfterClass(groups = "functional")
    public void destroy() {
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource("Pipeline_"));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource("LDCDEV_"));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(ingestionSource.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(generalSource.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(am.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(amLookup.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(dunsGuideBook.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(mlDaily.getSourceName()));
        purgeStrategyEntityMgr.delete(purgeStrategyEntityMgr.findStrategyBySource(matchResult.getSourceName()));

        for (String hdfsPath : hdfsPathsCleanup) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
                    HdfsUtils.rmdir(yarnConfiguration, hdfsPath);
                }
            } catch (IOException e) {
                log.error("Fail to cleanup " + hdfsPath);
            }
        }
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
        createHdfsPath(hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPathBuilder.constructSourceDir(srcName).toString());
        pipelineTempSource = new PurgeSource(srcName, hdfsPaths);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource("Pipeline_");
        strategy.setSourceType(SourceType.TEMP_SOURCE);
        strategy.setHdfsDays(3);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareOperationalSource() throws IOException {
        String srcName = "LDCDEV_SuspectRecords";
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(srcName, "2018-02-25_00-00-00_UTC").toString();

        createHdfsPath(hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPathBuilder.constructSourceDir(srcName).toString());
        operationalSource = new PurgeSource(srcName, hdfsPaths);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource("LDCDEV_");
        strategy.setSourceType(SourceType.TEMP_SOURCE);
        strategy.setHdfsDays(14);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareIngestion() throws IOException {
        String ingestionName = "TestIngestionToPurge";
        String hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-25_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-18_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        hdfsPath = hdfsPathBuilder.constructIngestionDir(ingestionName, "2018-02-11_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        List<String> hdfsPaths = Collections.singletonList(hdfsPath);
        ingestionSource = new PurgeSource(ingestionName, hdfsPaths);
        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(ingestionName);
        strategy.setSourceType(SourceType.INGESTION_SOURCE);
        strategy.setHdfsVersions(2);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareGeneralSource() throws IOException {
        String sourceName = "TestGeneralSourceToDelete";
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        String schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-25_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-18_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);
        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, "2018-02-11_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);
        List<String> hdfsPaths = Arrays.asList(hdfsPath, schemaPath);
        generalSource = new PurgeSource(sourceName, hdfsPaths);
        PurgeStrategy strategyToDelete = new PurgeStrategy();
        strategyToDelete.setSource(sourceName);
        strategyToDelete.setSourceType(SourceType.GENERAL_SOURCE);
        strategyToDelete.setHdfsDays(90);

        purgeStrategyEntityMgr.insertAll(Arrays.asList(strategyToDelete));
    }

    private void prepareAMSource(String sourceName) throws IOException {
        int retainedHdfsVersion = 2;
        List<DataCloudVersion> dcVersions = dataCloudVersionEntityMgr.allApprovedVerions();
        Collections.sort(dcVersions, DataCloudVersion.versionComparator);
        // Number of approved DataCloud version in LDC_ManageDB.DataCloudVersion
        // table should be >2
        String purgeDCVersion = dcVersions.get(0).getVersion();

        String retainVersion1 = null;
        String purgeVersion1 = null;
        switch (sourceName) {
        case DataCloudConstants.ACCOUNT_MASTER:
            retainVersion1 = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountMasterHdfsVersion();
            purgeVersion1 = dataCloudVersionEntityMgr.findVersion(purgeDCVersion).getAccountMasterHdfsVersion();
            break;
        case DataCloudConstants.ACCOUNT_MASTER_LOOKUP:
            retainVersion1 = dataCloudVersionEntityMgr.currentApprovedVersion().getAccountLookupHdfsVersion();
            purgeVersion1 = dataCloudVersionEntityMgr.findVersion(purgeDCVersion).getAccountLookupHdfsVersion();
            break;
        case DataCloudConstants.DUNS_GUIDE_BOOK:
            retainVersion1 = dataCloudVersionEntityMgr.currentApprovedVersion().getDunsGuideBookHdfsVersion();
            purgeVersion1 = null;
            break;
        default:
            throw new IllegalArgumentException("Unrecgonized source name for type AM_SOURCE");
        }
        String retainVersion2 = HdfsPathBuilder.dateFormat.format(new Date());
        String purgeVersion2 = "2000-01-01_00-00-00_UTC";

        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, retainVersion1).toString();
        String schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, retainVersion1).toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, retainVersion2).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, retainVersion2).toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);

        List<String> hdfsPaths = new ArrayList<>();
        if (purgeVersion1 != null) {
            hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, purgeVersion1).toString();
            schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, purgeVersion1).toString();
            createHdfsPath(hdfsPath);
            createHdfsPath(schemaPath);
            hdfsPaths.add(hdfsPath);
            hdfsPaths.add(schemaPath);
        }

        hdfsPath = hdfsPathBuilder.constructSnapshotDir(sourceName, purgeVersion2).toString();
        schemaPath = hdfsPathBuilder.constructSchemaDir(sourceName, purgeVersion2).toString();
        createHdfsPath(hdfsPath);
        createHdfsPath(schemaPath);
        hdfsPaths.add(hdfsPath);
        hdfsPaths.add(schemaPath);
        switch (sourceName) {
        case DataCloudConstants.ACCOUNT_MASTER:
            am = new PurgeSource(sourceName, hdfsPaths);
            break;
        case DataCloudConstants.ACCOUNT_MASTER_LOOKUP:
            amLookup = new PurgeSource(sourceName, hdfsPaths);
            break;
        case DataCloudConstants.DUNS_GUIDE_BOOK:
            dunsGuideBook = new PurgeSource(sourceName, hdfsPaths);
            break;
        default:
            throw new IllegalArgumentException("Unrecgonized source name for type AM_SOURCE");
        }

        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.AM_SOURCE);
        strategy.setHdfsVersions(retainedHdfsVersion);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }


    private void prepareMLSource() throws IOException {
        String sourceName = "TestMadisonLogic";
        String versionFormat = "yyyy-MM-dd";
        String hdfsBasePath = "/user/propdata/madison/dataflow/incremental";

        DateFormat df = new SimpleDateFormat(versionFormat);
        String versionToRetain = df.format(now);
        String hdfsPath = new Path(hdfsBasePath, versionToRetain).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            createHdfsPath(hdfsPath);
        }

        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE, -250);
        String versionToPurge = df.format(cal.getTime());

        hdfsPath = new Path(hdfsBasePath, versionToPurge).toString();
        if (!HdfsUtils.fileExists(yarnConfiguration, hdfsPath)) {
            createHdfsPath(hdfsPath);
        }
        mlDaily = new PurgeSource(sourceName, Arrays.asList(hdfsPath));

        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(sourceName);
        strategy.setSourceType(SourceType.TIMESERIES_SOURCE);
        strategy.setHdfsBasePath(hdfsBasePath);
        strategy.setVersionFormat(versionFormat);
        strategy.setHdfsDays(200);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareMatchResult() throws IOException {
        String srcName = "TestMatchResult";
        String hdfsBasePath = "/Pods/Production/Services/PropData/Matches";
        List<String> hdfsPaths = Arrays.asList( //
                new Path(hdfsBasePath, "1").toString(),
                new Path(hdfsBasePath, "2").toString());

        createHdfsPath(hdfsBasePath);
        for (String hdfsPath : hdfsPaths) {
            createHdfsPath(hdfsPath);
        }
        matchResult = new PurgeSource(srcName, hdfsPaths);

        PurgeStrategy strategy = new PurgeStrategy();
        strategy.setSource(srcName);
        strategy.setSourceType(SourceType.HDFS_DIR);
        strategy.setHdfsDays(14);
        strategy.setHdfsBasePath(hdfsBasePath);
        purgeStrategyEntityMgr.insertAll(Collections.singletonList(strategy));
    }

    private void prepareUnknownSource() throws IOException {
        String hdfsPath = hdfsPathBuilder.constructSnapshotDir(unknownSource, "2018-02-25_00-00-00_UTC").toString();
        createHdfsPath(hdfsPath);
    }

    private void prepareValidationMap() {
        validationMapNonDebugMode = new HashMap<>();
        validationMapNonDebugMode.put(getValidationKey(ingestionSource), ingestionSource);
        validationMapNonDebugMode.put(getValidationKey(am), am);
        validationMapNonDebugMode.put(getValidationKey(amLookup), amLookup);
        validationMapNonDebugMode.put(getValidationKey(dunsGuideBook), dunsGuideBook);
        validationMapNonDebugMode.put(getValidationKey(mlDaily), mlDaily);

        validationMapDebugMode = new HashMap<>();
        validationMapDebugMode.put(getValidationKey(pipelineTempSource), pipelineTempSource);
        validationMapDebugMode.put(getValidationKey(operationalSource), operationalSource);
        validationMapDebugMode.put(getValidationKey(ingestionSource), ingestionSource);
        validationMapDebugMode.put(getValidationKey(am), am);
        validationMapDebugMode.put(getValidationKey(amLookup), amLookup);
        validationMapDebugMode.put(getValidationKey(dunsGuideBook), dunsGuideBook);
        validationMapDebugMode.put(getValidationKey(mlDaily), mlDaily);
        validationMapDebugMode.put(getValidationKey(matchResult), matchResult);

        // generalSource should not be any of these map because upload
        // days is not old enough based on HdfsDays
    }

    private String getValidationKey(PurgeSource purgeSource) {
        return purgeSource.getSourceName();
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

            if (purgeSource.getSourceName().equals(mlDaily.getSourceName())) {
                validateMLSource(purgeSource);
            } else {
                Assert.assertTrue(isIdenticalList(expected.getHdfsPaths(), purgeSource.getHdfsPaths()));
                Assert.assertTrue(isIdenticalList(expected.getHiveTables(), purgeSource.getHiveTables()));
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
        Assert.assertTrue(hdfsPathSet.contains(mlDaily.getHdfsPaths().get(0)));
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        String versionToRetain = df.format(now);
        String hdfsPathToRetain = new Path("/user/propdata/madison/dataflow/incremental", versionToRetain).toString();
        Assert.assertFalse(hdfsPathSet.contains(hdfsPathToRetain));
    }

    private void createHdfsPath(String hdfsPath) throws IOException {
        HdfsUtils.mkdir(yarnConfiguration, hdfsPath);
        hdfsPathsCleanup.add(hdfsPath);
    }
}
