package com.latticeengines.datacloudapi.api.controller;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.source.impl.IngestionSource;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionAPIProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloud.etl.utils.SftpUtils;
import com.latticeengines.datacloud.etl.utils.TestSftpProvider;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.ingestion.SftpConfiguration;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.proxy.exposed.datacloudapi.IngestionProxy;

/**
 * dpltc deploy -a workflowapi,datacloudapi,eai
 */
public class IngestionResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(IngestionResourceDeploymentTestNG.class);

    private static final String POD_ID = IngestionResourceDeploymentTestNG.class.getSimpleName();
    private static final String DNB_INGESTION = "DnBCacheSeedTest";
    private static final String DNB_VERSION = "2019-05-01_00-00-00_UTC";
    private static final String DNB_FILE = "LE_SEED_OUTPUT_2019_05_001.OUT.gz";
    private static final String ORB_INGESTION = "OrbTest";
    private static final String BOMBORA_INGESTION = "BomboraTest";
    private static final String BOMBORA_FILE_VERSION1 = "20210114";
    private static final String BOMBORA_FILE_VERSION2 = "20210121";
    private static String bomboraTestVersion1;
    private static String bomboraTestVersion2;

    @Inject
    private IngestionEntityMgr ingestionEntityMgr;

    @Inject
    private IngestionProgressEntityMgr ingestionProgressEntityMgr;

    @Inject
    private IngestionProxy ingestionProxy;

    @Inject
    private IngestionVersionService ingestionVersionService;

    @Inject
    private IngestionAPIProviderService apiProviderService;

    @Inject
    private HdfsPathBuilder hdfsPathBuilder;

    @Inject
    private TestSftpProvider sftpProvider;

    private Map<String, Ingestion> ingestions = new HashMap<>();
    private String orbVersion;
    private int timeout = 2700000;

    // split into groups, to avoid too many jobs to congest yarn RM
    // IngestionName, Config, IngestionType
    private Object[][] getIngestions1() {
        return new Object[][] { { ORB_INGESTION, "{\"ClassName\":\"ApiConfiguration\",\"ConcurrentNum\":1,"
                + "\"VersionUrl\":\"http://api2.orb-intelligence.com/download/release-date.txt?api_key=ff7e84da-206c-4fb4-9c4f-b18fc4716e71\","
                + "\"VersionFormat\":\"EEE MMM dd HH:mm:ss zzz yyyy\","
                + "\"FileUrl\":\"http://api2.orb-intelligence.com/download/orb-db2-export-sample.zip?api_key=ff7e84da-206c-4fb4-9c4f-b18fc4716e71\","
                + "\"FileName\":\"orb-db2-export-sample.zip\"}", IngestionType.API } //
        };
    }

    private Object[][] getIngestions2() {
        return new Object[][] { { DNB_INGESTION,
                String.format(
                        "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,"
                                + "\"SftpHost\":\"%s\",\"SftpPort\":%d,\"SftpUsername\":\"%s\",\"SftpPassword\":\"%s\","
                                + "\"SftpDir\":\"home/sftpdev/ingest_test/SftpUtilsTestNG/DnB/gets\","
                                + "\"CheckVersion\":1,\"CheckStrategy\":\"ALL\","
                                + "\"HasSubfolder\":false,\"FileRegexPattern\":\"LE_SEED_OUTPUT_(.+).OUT.gz\","
                                + "\"FileTSPattern\":\"yyyy_MM\"}",
                        sftpProvider.getSftpHost(), sftpProvider.getSftpPort(), sftpProvider.getSftpUserName(),
                        sftpProvider.getSftpPassword()),
                IngestionType.SFTP }, };
    }

    private Object[][] getIngestions3() {
        return new Object[][] { { BOMBORA_INGESTION,
                String.format("{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":1,"
                        + "\"SftpHost\":\"%s\",\"SftpPort\":%d,\"SftpUsername\":\"%s\",\"SftpPassword\":\"%s\","
                        + "\"SftpDir\":\"home/sftpdev/ingest_test/SftpUtilsTestNG/Bombora/bombora-clientfiles-adat_zip\","
                        + "\"CheckVersion\":1,\"CheckStrategy\":\"WEEK\","
                        + "\"HasSubfolder\":true,\"SubfolderRegexPattern\":\"\\\\d{8}\",\"SubfolderTSPattern\":\"yyyyMMdd\","
                        + "\"FileRegexPattern\":\"AllDomainsAllTopicsZips_(.+).csv.gz\"}", sftpProvider.getSftpHost(),
                        sftpProvider.getSftpPort(), sftpProvider.getSftpUserName(), sftpProvider.getSftpPassword()),
                IngestionType.SFTP }, };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version, Size
    private static Object[][] getExpectedResult1() {
        return new Object[][] { //
                { ORB_INGESTION, 1, null, null } //
        };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version, Size
    private static Object[][] getExpectedResult2() {
        return new Object[][] { //
                { DNB_INGESTION, 1, DNB_VERSION, null } };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version, Size
    private static Object[][] getExpectedResult3() {
        return new Object[][] { //
                { BOMBORA_INGESTION, 1, null, null } };
    }

    @BeforeClass(groups = "deployment")
    public void setup() {
        prepareCleanPod(POD_ID);
        prepareIngestions();
        prepareBomboraFiles();
    }

    @AfterClass(groups = "deployment")
    public void destroy() {
        for (Ingestion ingestion : ingestions.values()) {
            ingestionEntityMgr.delete(ingestion);
        }
        recoverBomboraFiles();
    }

    @Test(groups = "deployment")
    public void testIngestions() {
        // no sequential dependencies among groups
        // can comment out any group(s)
        testIngestions1();
        testIngestions2();
        testIngestion3();
    }

    // group1: Orb
    private void testIngestions1() {
        log.info("Test ingestion group 1: Orb");
        Ingestion orbIngestion = ingestions.get(ORB_INGESTION);
        orbIngestion.setSchedularEnabled(true);
        ingestionEntityMgr.save(orbIngestion);

        ApiConfiguration apiConfig = (ApiConfiguration) orbIngestion.getProviderConfiguration();
        orbVersion = apiProviderService.getTargetVersion(apiConfig);

        scan();

        verifyIngestions(getExpectedResult1());
    }

    // group2: DnB
    private void testIngestions2() {
        log.info("Test ingestion group 2: DnB");
        Ingestion dnbIngestion = ingestions.get(DNB_INGESTION);
        dnbIngestion.setSchedularEnabled(true);
        ingestionEntityMgr.save(dnbIngestion);

        IngestionRequest request = new IngestionRequest();
        request.setSubmitter(PropDataConstants.SCAN_SUBMITTER);
        request.setFileName(DNB_FILE);
        request.setStartNow(true);
        IngestionProgress progress = ingestionProxy.ingestInternal(DNB_INGESTION, request, POD_ID);
        Assert.assertNotNull(progress);
        Assert.assertNotNull(progress.getApplicationId());
        scan();

        verifyIngestions(getExpectedResult2());
    }

    // group3: Bombora
    private void testIngestion3() {
        log.info("Test ingestion group 3: Bombora");
        Ingestion bomboraIngestion = ingestions.get(BOMBORA_INGESTION);
        bomboraIngestion.setSchedularEnabled(true);
        ingestionEntityMgr.save(bomboraIngestion);

        // prepare one version on hdfs, expect to ingest the other version
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        df.setTimeZone(TimeZone.getTimeZone(HdfsPathBuilder.UTC));
        try {
            uploadBaseSourceFile(new IngestionSource(BOMBORA_INGESTION), "AllDomainsAllTopicsZips_20190721_1.csv.gz",
                    HdfsPathBuilder.dateFormat.format(df.parse(bomboraTestVersion2)));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }

        scan();

        Object[][] expectedResults = getExpectedResult3();
        try {
            // popupate expected version to ingest
            expectedResults[0][2] = HdfsPathBuilder.dateFormat.format(df.parse(bomboraTestVersion1));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        verifyIngestions(expectedResults);
    }

    private void prepareIngestions() {
        createIngestions(getIngestions1());
        createIngestions(getIngestions2());
        createIngestions(getIngestions3());
    }

    /**
     * Bombora files/subfolders are scanned based on timestamp in the name.
     * Rename bombora files/subfolders on testing SFTP to be testable for
     * current date.
     */
    private void prepareBomboraFiles() {
        Date current = new Date();
        LocalDateTime localDateTime = current.toInstant().atZone(ZoneId.systemDefault()).toLocalDateTime();
        Date lastWeek = Date.from(localDateTime.plusWeeks(-1).atZone(ZoneId.systemDefault()).toInstant());
        DateFormat df = new SimpleDateFormat("yyyyMMdd");
        bomboraTestVersion1 = df.format(lastWeek);
        bomboraTestVersion2 = df.format(current);
        renameBomboraFile(BOMBORA_FILE_VERSION1, bomboraTestVersion1);
        renameBomboraFile(BOMBORA_FILE_VERSION2, bomboraTestVersion2);
    }

    /**
     * Recover bombora files/subfolders on testing SFTP to orginal names
     */
    private void recoverBomboraFiles() {
        renameBomboraFile(bomboraTestVersion1, BOMBORA_FILE_VERSION1);
        renameBomboraFile(bomboraTestVersion2, BOMBORA_FILE_VERSION2);
    }

    private void renameBomboraFile(String oldPath, String newPath) {
        SftpUtils.renamePath((SftpConfiguration) ingestions.get(BOMBORA_INGESTION).getProviderConfiguration(), oldPath,
                newPath);
    }

    private void createIngestions(Object[][] ingestionData) {
        for (Object[] data : ingestionData) {
            Ingestion existing = ingestionEntityMgr.getIngestionByName((String) data[0]);
            if (existing != null) {
                ingestionEntityMgr.delete(existing);
            }
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName((String) data[0]);
            ingestion.setConfig((String) data[1]);
            // ingestion is enabled in each test
            ingestion.setSchedularEnabled(false);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(1);
            ingestion.setIngestionType((IngestionType) data[2]);
            ingestionEntityMgr.save(ingestion);
            ingestions.put(ingestion.getIngestionName(),
                    ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        }
    }

    private void scan() {
        List<IngestionProgress> progresses = ingestionProxy.scan(POD_ID);
        List<String> appIds = progresses.stream() //
                .map(IngestionProgress::getApplicationId).collect(Collectors.toList());
        log.info("Kicked off applications: " + StringUtils.join(appIds) + ", and wait 3 sec.");
        try {
            Thread.sleep(3000L);
        } catch (InterruptedException e) {
            log.warn("Sleep was interrupted", e);
        }
    }

    private void verifyIngestions(Object[][] expectedResults) {
        List<Runnable> verifiers = new ArrayList<>();
        for (Object[] data : expectedResults) {
            String name = (String) data[0];
            int expectedProgresses = (int) data[1];
            String version = (String) data[2];
            Integer size = (Integer) data[3];
            if (expectedResults.length > 1) {
                Runnable runnable = () -> verifyIngest(name, expectedProgresses, version, size);
                verifiers.add(runnable);
            } else {
                verifyIngest(name, expectedProgresses, version, size);
            }
        }
        if (CollectionUtils.isNotEmpty(verifiers)) {
            ThreadPoolUtils.runInParallel(verifiers);
        }
    }

    private void verifyIngest(String name, int expectedProgresses, String version, Integer size) {
        HdfsPodContext.changeHdfsPodId(POD_ID);
        if (name.equals(ORB_INGESTION)) {
            version = orbVersion;
        }
        Ingestion ingestion = ingestionEntityMgr.getIngestionByName(name);
        Map<String, Object> fields = new HashMap<>();
        fields.put("IngestionId", ingestion.getPid());
        fields.put("Version", version);
        List<IngestionProgress> progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        Assert.assertEquals(progresses.size(), expectedProgresses);
        long startTime = System.currentTimeMillis();
        ProgressStatus status = ingestionVersionService.findProgressAtVersion(name, version);
        while (status != ProgressStatus.FINISHED && status != ProgressStatus.FAILED
                && System.currentTimeMillis() - startTime <= timeout) {
            ingestionProxy.scan(POD_ID);
            status = ingestionVersionService.findProgressAtVersion(name, version);
            SleepUtils.sleep(60000L);
        }
        Assert.assertEquals(status, ProgressStatus.FINISHED);
        progresses = ingestionProgressEntityMgr.findProgressesByField(fields, null);
        for (IngestionProgress progress : progresses) {
            assertProgress(ingestion, progress, size, version);
        }
        String ingestionPath = hdfsPathBuilder.constructIngestionDir(name, version).toString();
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, ingestionPath);
            log.info("Ingested files: " + String.join(",", files));
            Assert.assertTrue(files.stream().anyMatch(file -> file.contains(HdfsPathBuilder.SUCCESS_FILE)));
            Assert.assertEquals(files.size() - 1, expectedProgresses);
        } catch (IOException e) {
            throw new RuntimeException("Fail to ", e);
        }
    }

    private void assertProgress(Ingestion ingestion, IngestionProgress progress, Integer size, String version) {
        ApplicationId appId = ApplicationIdUtils.toApplicationIdObj(progress.getApplicationId());
        FinalApplicationStatus appStatus = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 3600);
        Assert.assertEquals(appStatus, FinalApplicationStatus.SUCCEEDED);
        Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
        if (size != null) {
            Assert.assertEquals(progress.getSize().intValue(), size.intValue());
        }
        String currentVersion = ingestionVersionService.findCurrentVersion(ingestion);
        Assert.assertTrue(currentVersion.compareTo(version) >= 0);
    }
}
