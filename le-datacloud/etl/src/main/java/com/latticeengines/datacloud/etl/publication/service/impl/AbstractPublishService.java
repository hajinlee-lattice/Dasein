package com.latticeengines.datacloud.etl.publication.service.impl;

import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.etl.publication.service.PublicationProgressService;
import com.latticeengines.datacloud.etl.publication.service.PublishConfigurationParser;
import com.latticeengines.domain.exposed.datacloud.manage.PublicationProgress;

abstract class AbstractPublishService {

    private static Logger log = LoggerFactory.getLogger(AbstractPublishService.class);
    private static final Integer HANGING_THRESHOLD_HOURS = 24;
    private static final Integer MAX_ERRORS = 100;

    @Inject
    protected PublicationProgressService progressService;

    @Inject
    protected PublishConfigurationParser configurationParser;

    @Inject
    private YarnClient yarnClient;

    protected FinalApplicationStatus waitForApplicationToFinish(ApplicationId appId, PublicationProgress progress) {
        Long timeout = TimeUnit.HOURS.toMillis(HANGING_THRESHOLD_HOURS);
        int errors = 0;
        FinalApplicationStatus status = FinalApplicationStatus.UNDEFINED;
        do {
            try {
                ApplicationReport report = yarnClient.getApplicationReport(appId);
                status = report.getFinalApplicationStatus();
                YarnApplicationState state = report.getYarnApplicationState();
                Float appProgress = report.getProgress();
                String logMessage = String.format("Application [%s] is at state [%s]", appId, state);
                if (YarnApplicationState.RUNNING.equals(state)) {
                    logMessage += String.format(": %.2f ", appProgress * 100) + "%";
                }
                log.info(logMessage);

                appProgress = convertToOverallProgress(report.getProgress());
                Float dbProgress = progress.getProgress();
                if (!appProgress.equals(dbProgress)) {
                    progress = progressService.update(progress).progress(appProgress).commit();
                } else {
                    Long lastUpdate = progress.getLatestStatusUpdate().getTime();
                    if (System.currentTimeMillis() - lastUpdate >= timeout) {
                        String errorMsg = "The process has been hanging for " + DurationFormatUtils
                                .formatDurationWords(System.currentTimeMillis() - lastUpdate, true, false) + ".";
                        throw new RuntimeException(errorMsg);
                    }
                }
            } catch (Exception e) {
                log.error(e.getMessage(), e);
                errors++;
                if (errors >= MAX_ERRORS) {
                    throw new RuntimeException("Exceeded maximum error allowance.", e);
                }
            } finally {
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        } while (!YarnUtils.TERMINAL_STATUS.contains(status));
        return status;
    }

    private Float convertToOverallProgress(Float applicationProgress) {
        return applicationProgress * 0.9f + 0.05f;
    }

}
