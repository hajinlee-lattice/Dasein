package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.service.RefreshService;

public class RefreshExecutor implements RefreshJobExecutor {

    private Log log = LogFactory.getLog(this.getClass());

    private final RefreshService refreshService;
    private static final int MAX_RETRY = 50;
    private final String jobSubmitter;

    public RefreshExecutor(RefreshService refreshService) {
        this.refreshService = refreshService;
        this.jobSubmitter = refreshService.getClass().getSimpleName();
    }

    @Override
    public synchronized void proceedProgress(Progress progress) {
        RefreshProgress refreshProgress = (RefreshProgress) progress;
        refreshProgress = retryJob(refreshProgress);
        switch (refreshProgress.getStatus()) {
            case NEW:
                refreshProgress = refreshService.transform(refreshProgress);
                break;
            case TRANSFORMED:
                refreshProgress = refreshService.exportToDB(refreshProgress);
                break;
            case UPLOADED:
                refreshProgress = refreshService.finish(refreshProgress);
                break;
            default:
                log.warn(String.format("Illegal starting status %s for progress %s",
                        refreshProgress.getStatus(), refreshProgress.getRootOperationUID()));
        }
        if (refreshProgress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(refreshProgress);
        } else {
            logJobSucceed(refreshProgress);
        }
    }

    @Override
    public void kickOffNewProgress() {
        int retries = 0;
        while (retries++ < MAX_RETRY) {
            try {
                String baseVersion =  refreshService.findBaseVersionForNewProgress();

                if (baseVersion != null) {
                    refreshService.startNewProgress(new Date(), baseVersion, jobSubmitter);
                    return;
                }

                Thread.sleep(1800 * 1000L);
            } catch (Exception e) {
                log.error(e);
            }
        }
        log.error("Failed to find a chance to kick off a refresh of " + refreshService.getSource().getSourceName()
            + " after " + MAX_RETRY + " retries with 0.5 hour intervals.");
    }

    private RefreshProgress retryJob(RefreshProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            ProgressStatus resumeStatus;
            switch (progress.getStatusBeforeFailed()) {
                case NEW:
                case TRANSFORMING:
                    resumeStatus = ProgressStatus.NEW;
                    break;
                case TRANSFORMED:
                case UPLOADING:
                    resumeStatus = ProgressStatus.TRANSFORMED;
                    break;
                default:
                    resumeStatus = ProgressStatus.NEW;
            }
            progress.setStatus(resumeStatus);
            progress.setNumRetries(progress.getNumRetries() + 1);
        }
        return progress;
    }

    private void logJobSucceed(RefreshProgress progress) {
        log.info("Refreshing " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() + " RootOperationUID=" + progress.getRootOperationUID());
    }

    private void logJobFailed(RefreshProgress progress) {
        log.error("Refreshing " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() +" RootOperationUID=" + progress.getRootOperationUID());
    }
}
