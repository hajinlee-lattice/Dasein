package com.latticeengines.propdata.workflow.engine.steps;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.route.CamelRouteConfiguration;
import com.latticeengines.domain.exposed.propdata.ingestion.Protocol;
import com.latticeengines.domain.exposed.propdata.manage.Ingestion;
import com.latticeengines.domain.exposed.propdata.manage.IngestionProgress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.core.service.impl.HdfsPodContext;
import com.latticeengines.propdata.engine.ingestion.service.IngestionProgressService;
import com.latticeengines.proxy.exposed.eai.EaiProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("ingestionStep")
@Scope("prototype")
public class IngestionStep extends BaseWorkflowStep<IngestionStepConfiguration> {
    private static final Log log = LogFactory.getLog(IngestionStep.class);

    private IngestionProgress progress;

    @Autowired
    private IngestionProgressService ingestionProgressService;

    @Autowired
    private EaiProxy eaiProxy;

    private YarnClient yarnClient;

    private static final long WORKFLOW_WAIT_TIME_IN_MILLIS = TimeUnit.MINUTES.toMillis(90);

    private static final long MAX_MILLIS_TO_WAIT = 1000L * 60 * 20;

    @Override
    public void execute() {
        try {
            log.info("Entering IngestionStep execute()");
            progress = getConfiguration().getIngestionProgress();
            HdfsPodContext.changeHdfsPodId(progress.getHdfsPod());
            Ingestion ingestion = getConfiguration().getIngestion();
            Protocol protocol = getConfiguration().getProtocol();
            ingestion.setProtocol(protocol);
            progress.setIngestion(ingestion);
            initializeYarnClient();
            ingestionByCamelRoute();
            log.info("Exiting IngestionStep execute()");
        } catch (Exception e) {
            failByException(e);
        } finally {
            try {
                yarnClient.close();
            } catch (Exception e) {
                log.error(e);
            }
        }
    }

    private void ingestionByCamelRoute() throws Exception {
        CamelRouteConfiguration camelRouteConfig = ingestionProgressService
                .createCamelRouteConfiguration(progress);
        ImportConfiguration importConfig = ImportConfiguration
                .createForCamelRouteConfiguration(camelRouteConfig);
        Path hdfsDir = new Path(progress.getDestination()).getParent();
        if (HdfsUtils.fileExists(yarnConfiguration, progress.getDestination())) {
            HdfsUtils.rmdir(yarnConfiguration, progress.getDestination());
        }
        Path downloadFlag = new Path(hdfsDir, EngineConstants.INGESTION_DOWNLOADING);
        if (HdfsUtils.fileExists(yarnConfiguration, downloadFlag.toString())) {
            HdfsUtils.rmdir(yarnConfiguration, downloadFlag.toString());
        }
        Path successFlag = new Path(hdfsDir, EngineConstants.INGESTION_SUCCESS);
        if (HdfsUtils.fileExists(yarnConfiguration, successFlag.toString())) {
            HdfsUtils.rmdir(yarnConfiguration, successFlag.toString());
        }
        AppSubmission submission = eaiProxy.createImportDataJob(importConfig);
        String eaiAppId = submission.getApplicationIds().get(0);
        log.info("EAI Service ApplicationId: " + eaiAppId);
        FinalApplicationStatus status = waitForStatus(eaiAppId, WORKFLOW_WAIT_TIME_IN_MILLIS,
                FinalApplicationStatus.SUCCEEDED);
        if (status == FinalApplicationStatus.SUCCEEDED
                && waitForFileToBeDownloaded(progress.getDestination())) {
            Long size = HdfsUtils.getFileSize(yarnConfiguration, progress.getDestination());
            progress = ingestionProgressService.updateProgress(progress).size(size)
                    .status(ProgressStatus.FINISHED).commit(true);
            if (HdfsUtils.fileExists(yarnConfiguration, downloadFlag.toString())) {
                try (FileSystem fs = FileSystem.newInstance(yarnConfiguration)) {
                    fs.rename(downloadFlag, successFlag);
                }
            }
            log.info("Ingestion finished. Progress: " + progress.toString());
        } else {
            progress = ingestionProgressService.updateProgress(progress)
                    .status(ProgressStatus.FAILED).commit(true);
            log.error("Ingestion failed. Progress: " + progress.toString());
        }
    }

    private void failByException(Exception e) {
        progress = ingestionProgressService.updateProgress(progress).status(ProgressStatus.FAILED)
                .errorMessage(e.getMessage()).commit(true);
        log.error("Ingestion failed: " + e.getMessage() + ". Progress: " + progress.toString());
    }

    private void initializeYarnClient() {
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(yarnConfiguration);
        yarnClient.start();
    }

    private FinalApplicationStatus waitForStatus(String applicationId, Long waitTimeInMillis,
            FinalApplicationStatus... applicationStatuses) throws Exception {
        waitTimeInMillis = waitTimeInMillis == null ? MAX_MILLIS_TO_WAIT : waitTimeInMillis;
        log.info(String.format("Waiting on %s for at most %dms.", applicationId, waitTimeInMillis));

        FinalApplicationStatus status = null;
        long start = System.currentTimeMillis();

        done: do {
            ApplicationReport report = yarnClient
                    .getApplicationReport(ConverterUtils.toApplicationId(applicationId));
            status = report.getFinalApplicationStatus();
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

    private boolean waitForFileToBeDownloaded(String destPath) {
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
}
