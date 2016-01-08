package com.latticeengines.propdata.collection.service.impl;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.service.RefreshService;

public class RefreshExecutor implements RefreshJobExecutor {

    private Log log = LogFactory.getLog(this.getClass());

    private final RefreshService refreshService;

    public RefreshExecutor(RefreshService refreshService) {
        this.refreshService = refreshService;
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
    public synchronized void print() {
        try {
            String uuid = UUID.randomUUID().toString();
            Random random = new Random();
            System.out.println(refreshService + " - " + uuid + " start.");
            Thread.sleep(random.nextInt(3000));
            System.out.println(refreshService + " - " + uuid + " finished.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
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
