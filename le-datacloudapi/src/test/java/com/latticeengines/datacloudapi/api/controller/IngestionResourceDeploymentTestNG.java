package com.latticeengines.datacloudapi.api.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.core.util.PropDataConstants;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.entitymgr.IngestionProgressEntityMgr;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionAPIProviderService;
import com.latticeengines.datacloud.etl.ingestion.service.IngestionVersionService;
import com.latticeengines.datacloudapi.api.testframework.PropDataApiDeploymentTestNGBase;
import com.latticeengines.domain.exposed.datacloud.ingestion.ApiConfiguration;
import com.latticeengines.domain.exposed.datacloud.ingestion.IngestionRequest;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion;
import com.latticeengines.domain.exposed.datacloud.manage.Ingestion.IngestionType;
import com.latticeengines.domain.exposed.datacloud.manage.IngestionProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.proxy.exposed.datacloudapi.IngestionProxy;

/**
 * dpltc deploy -a workflowapi,datacloudapi,eai
 */
@Component
public class IngestionResourceDeploymentTestNG extends PropDataApiDeploymentTestNGBase {
    private static Logger log = LoggerFactory.getLogger(IngestionResourceDeploymentTestNG.class);

    private static final String POD_ID = IngestionResourceDeploymentTestNG.class.getSimpleName();
    private static final String DNB_INGESTION = "DnBCacheSeedTest";
    private static final String DNB_VERSION = "2016-08-01_00-00-00_UTC";
    private static final String DNB_FILE = "LE_SEED_OUTPUT_2016_08_003.OUT.gz";
    private static final String ORB_INGESTION = "OrbTest";

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

    private ExecutorService verificationWorkers;
    private List<Ingestion> ingestions = new ArrayList<>();
    private String orbVersion;
    private int timeout = 2700000;

    // split into groups, to avoid too many jobs to congest yarn RM
    // IngestionName, Config, IngestionType
    private static Object[][] getIngestions1() {
        return new Object[][] {
                { ORB_INGESTION,
                        "{\"ClassName\":\"ApiConfiguration\",\"ConcurrentNum\":1,\"VersionUrl\":\"http://api2.orb-intelligence.com/download/release-date.txt?api_key=ff7e84da-206c-4fb4-9c4f-b18fc4716e71\",\"VersionFormat\":\"EEE MMM dd HH:mm:ss zzz yyyy\",\"FileUrl\":\"http://api2.orb-intelligence.com/download/orb-db2-export-sample.zip?api_key=ff7e84da-206c-4fb4-9c4f-b18fc4716e71\",\"FileName\":\"orb-db2-export-sample.zip\"}",
                        IngestionType.API } //
        };
    }

    private static Object[][] getIngestions2() {
        return new Object[][] {
                { DNB_INGESTION,
                        "{\"ClassName\":\"SftpConfiguration\",\"ConcurrentNum\":2,\"SftpHost\":\"10.141.1.239\",\"SftpPort\":22,\"SftpUsername\":\"sftpdev\",\"SftpPassword\":\"KPpl2JWz+k79LWvYIKz6cA==\",\"SftpDir\":\"/ingest_test/dnb\",\"CheckVersion\":1,\"CheckStrategy\":\"ALL\",\"FileExtension\":\"OUT.gz\",\"FileNamePrefix\":\"LE_SEED_OUTPUT_\",\"FileNamePostfix\":\"(.*)\",\"FileTimestamp\":\"yyyy_MM\"}",
                        IngestionType.SFTP },
        };
    }

    // IngestionName, ExpectedCreatedProgressNum, Version
    private static Object[][] getExpectedResult1() {
        return new Object[][] { //
                { ORB_INGESTION, 1, null, null } //
        };
    }

    private static Object[][] getExpectedResult2() {
        return new Object[][] { //
                { DNB_INGESTION, 3, DNB_VERSION, null }
        };
    }

    @BeforeClass(groups = "deployment")
    public void setup() {
        prepareCleanPod(POD_ID);
        verificationWorkers = ThreadPoolUtils.getFixedSizeThreadPool("ingestion-test", 2);
    }

    @AfterClass(groups = "deployment")
    public void destroy() {
        for (Ingestion ingestion : ingestions) {
            ingestionEntityMgr.delete(ingestion);
        }
        verificationWorkers.shutdownNow();
    }

    @Test(groups = "deployment")
    public void testIngestions() {
        // no sequential dependencies among groups
        // can comment out any group(s)
        testIngestions1();
        testIngestions2();
    }

    // group1: Orb
    private void testIngestions1() {
        log.info("Test ingestion group 1: Orb");
        createIngestions(getIngestions1());
        Ingestion orbIngestion = ingestionEntityMgr.getIngestionByName(ORB_INGESTION);
        ApiConfiguration apiConfig = (ApiConfiguration) orbIngestion.getProviderConfiguration();
        orbVersion = apiProviderService.getTargetVersion(apiConfig);

        scan();

        verifyIngestions(getExpectedResult1());
    }

    // group2: DnB
    private void testIngestions2() {
        log.info("Test ingestion group 2: DnB");
        createIngestions(getIngestions2());

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

    private void createIngestions(Object[][] ingestionData) {
        for (Object[] data : ingestionData) {
            Ingestion existing = ingestionEntityMgr.getIngestionByName((String) data[0]);
            if (existing != null) {
                ingestionEntityMgr.delete(existing);
            }
            Ingestion ingestion = new Ingestion();
            ingestion.setIngestionName((String) data[0]);
            ingestion.setConfig((String) data[1]);
            ingestion.setSchedularEnabled(Boolean.TRUE);
            ingestion.setNewJobRetryInterval(10000L);
            ingestion.setNewJobMaxRetry(1);
            ingestion.setIngestionType((IngestionType) data[2]);
            ingestionEntityMgr.save(ingestion);
            ingestions.add(ingestionEntityMgr.getIngestionByName(ingestion.getIngestionName()));
        }
    }

    private void scan() {
        List<IngestionProgress> progresses = ingestionProxy.scan(POD_ID);
        Assert.assertTrue(CollectionUtils.isNotEmpty(progresses));
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
            ThreadPoolUtils.runRunnablesInParallel(verificationWorkers, verifiers, 60, 1);
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
            try {
                Thread.sleep(60000L);
            } catch (InterruptedException e) {
                // Do nothing for InterruptedException
            }
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
        ApplicationId appId = ApplicationId.fromString(progress.getApplicationId());
        FinalApplicationStatus appStatus = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 3600);
        Assert.assertEquals(appStatus, FinalApplicationStatus.SUCCEEDED);
        Assert.assertEquals(progress.getStatus(), ProgressStatus.FINISHED);
        if (size != null) {
            Assert.assertEquals(progress.getSize().intValue(), size.intValue());
        }
        String currentVersion = ingestionVersionService.findCurrentVersion(ingestion);
        Assert.assertEquals(currentVersion, version);
    }
}
