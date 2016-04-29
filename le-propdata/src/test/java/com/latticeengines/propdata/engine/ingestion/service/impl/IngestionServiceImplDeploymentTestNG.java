package com.latticeengines.propdata.engine.ingestion.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import java.util.Date;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.yarn.client.YarnClient;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.IngestionNames;
import com.latticeengines.propdata.core.PropDataConstants;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.propdata.engine.ingestion.service.IngestionService;
import com.latticeengines.propdata.engine.testframework.PropDataEngineDeploymentTestNGBase;
import com.latticeengines.proxy.exposed.eai.EaiProxy;

public class IngestionServiceImplDeploymentTestNG extends PropDataEngineDeploymentTestNGBase {

    private static final Log log = LogFactory.getLog(IngestionServiceImplDeploymentTestNG.class);

    private static final String INGESTION_NAME = IngestionNames.BOMBORA_FIREHOSE;
    private static final String HDFS_POD = "FunctionalBomboraFireHose";
    private static final String FILE_NAME = "Bombora_Firehose_20160422.csv.gz";
    private static final String TEST_SUBMITTER = PropDataConstants.SCAN_SUBMITTER;
    private static final long WORKFLOW_WAIT_TIME_IN_MILLIS = TimeUnit.MINUTES.toMillis(90);
    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 20;

    @Autowired
    private YarnClient yarnClient;

    @Autowired
    private EaiProxy eaiProxy;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private IngestionService ingestionService;

    private Ingestion ingestion;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        ingestion = ingestionService.getIngestionByName(INGESTION_NAME);
        prepareCleanPod(HDFS_POD);
    }

    @Test(groups = "deployment", enabled = true)
    public void testScan() throws Exception {
        List<IngestionProgress> progresses = ingestionService.scan(HDFS_POD);
        log.info("Number of progresses starting: " + progresses.size());
        for (IngestionProgress progress : progresses) {
            log.info("Ingestion Progress: " + progress.toString());
        }
        for (IngestionProgress progress : progresses) {
            Assert.assertNotNull(progress.getApplicationId());
            FinalApplicationStatus status = waitForStatus(progress.getApplicationId(),
                    WORKFLOW_WAIT_TIME_IN_MILLIS, FinalApplicationStatus.SUCCEEDED);
            Assert.assertEquals(status, FinalApplicationStatus.SUCCEEDED);
            Assert.assertTrue(waitForFileToBeDownloaded(progress.getDestination()),
                    "Could not find the file to be downloaded");
        }
    }

    @Test(groups = "deployment", enabled = false)
    public void testSftpToHhfsRoute() throws Exception {
        IngestionProgress progress = createDataIngestionProgess(ingestion);
        CamelRouteConfiguration camelRouteConfig = ingestionProgressService
                .createCamelRouteConfiguration(progress);
        ImportConfiguration importConfig = ImportConfiguration
                .createForCamelRouteConfiguration(camelRouteConfig);
        AppSubmission submission = eaiProxy.createImportDataJob(importConfig);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        log.info("ApplicationId: " + appId);

        FinalApplicationStatus status = waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        Assert.assertTrue(waitForFileToBeDownloaded(progress.getDestination()),
                "Could not find the file to be downloaded");
    }

    protected IngestionProgress createDataIngestionProgess(Ingestion ingestion) {
        IngestionProgress progress = new IngestionProgress();
        progress.setApplicationId(UUID.randomUUID().toString().toUpperCase());
        String source = ingestionProgressService.constructSource(ingestion, FILE_NAME);
        progress.setSource(source);
        String destination = ingestionProgressService.constructDestination(ingestion, FILE_NAME);
        progress.setDestination(destination);
        progress.setIngestion(ingestion);
        progress.setHdfsPod(HDFS_POD);
        progress.setLastestStatusUpdateTime(new Date());
        progress.setRetries(0);
        progress.setSize(Long.valueOf("1000"));
        progress.setStartTime(new Date());
        progress.setStatus(ProgressStatus.NEW);
        progress.setTriggeredBy(TEST_SUBMITTER);
        return progress;
    }

    protected boolean waitForFileToBeDownloaded(String destPath) {
        Long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < 10000) {
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, destPath)) {
                    return true;
                }
                Thread.sleep(1000L);
            } catch (Exception e) {
                // ignore
            }
        }
        return false;
    }

    protected FinalApplicationStatus waitForStatus(String applicationId, Long waitTimeInMillis,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        Assert.assertNotNull(yarnClient, "Yarn client must be set");
        Assert.assertNotNull(applicationId, "ApplicationId must not be null");
        waitTimeInMillis = waitTimeInMillis == null ? MAX_MILLIS_TO_WAIT : waitTimeInMillis;
        log.info(String.format("Waiting on %s for at most %dms.", applicationId, waitTimeInMillis));

        FinalApplicationStatus status = null;
        long start = System.currentTimeMillis();

        done: do {
            status = findStatus(yarnClient, applicationId);
            if (status == null) {
                break;
            }
            for (FinalApplicationStatus statusCheck : applicationStatuses) {
                if (status.equals(statusCheck) || YarnUtils.TERMINAL_STATUS.contains(status)) {
                    break done;
                }
            }
            Thread.sleep(1000);
        } while (System.currentTimeMillis() - start < waitTimeInMillis);
        return status;
    }

    private FinalApplicationStatus findStatus(YarnClient client, String applicationId) {
        FinalApplicationStatus status = null;
        for (ApplicationReport report : client.listApplications()) {
            if (report.getApplicationId().toString().equals(applicationId)) {
                status = report.getFinalApplicationStatus();
                break;
            }
        }
        return status;
    }

}
