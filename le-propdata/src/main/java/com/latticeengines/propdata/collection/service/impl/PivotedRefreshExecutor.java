package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;

public class PivotedRefreshExecutor implements RefreshJobExecutor {

    private Log log = LogFactory.getLog(this.getClass());

    private final PivotService pivotService;

    public PivotedRefreshExecutor(PivotService pivotService) {
        this.pivotService = pivotService;
    }

    @Override
    public synchronized void proceedProgress(Progress progress) {
        PivotProgress pivotProgress = (PivotProgress) progress;
        progress = retryJob(pivotProgress);
        switch (progress.getStatus()) {
            case NEW:
                progress = pivotService.pivot(pivotProgress);
                break;
            case PIVOTED:
                progress = pivotService.exportToDB(pivotProgress);
                break;
            case UPLOADED:
                progress = pivotService.finish(pivotProgress);
                break;
            default:
                log.warn(String.format("Illegal starting status %s for progress %s",
                        progress.getStatus(), progress.getRootOperationUID()));
        }
        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(pivotProgress);
        } else {
            logJobSucceed(pivotProgress);
        }
    }

    private PivotProgress retryJob(PivotProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            ProgressStatus resumeStatus;
            switch (progress.getStatusBeforeFailed()) {
                case NEW:
                case PIVOTING:
                    resumeStatus = ProgressStatus.NEW;
                    break;
                case PIVOTED:
                case UPLOADING:
                    resumeStatus = ProgressStatus.PIVOTED;
                    break;
                default:
                    resumeStatus = ProgressStatus.NEW;
            }
            progress.setStatus(resumeStatus);
        }
        return progress;
    }

    private void logJobSucceed(PivotProgress progress) {
        log.info("Pivoting " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() + " RootOperationUID=" + progress.getRootOperationUID());
    }

    private void logJobFailed(PivotProgress progress) {
        log.error("Pivoting " + progress.getSourceName() + " finished for date " +
                progress.getPivotDate() +" RootOperationUID=" + progress.getRootOperationUID());
    }
}
