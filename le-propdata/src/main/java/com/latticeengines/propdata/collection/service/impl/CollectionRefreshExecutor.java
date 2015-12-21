package com.latticeengines.propdata.collection.service.impl;

import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.util.DateRange;

public class CollectionRefreshExecutor implements RefreshJobExecutor {

    private static final String jobSubmitter = ProgressOrchestrator.class.getSimpleName();

    private final ArchiveService archiveService;
    private static final Log log = LogFactory.getLog(CollectionRefreshExecutor.class);

    public CollectionRefreshExecutor(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    @Override
    public synchronized void proceedProgress(Progress progress) {
        ArchiveProgress archiveProgress = (ArchiveProgress) progress;
        progress = retryJob(archiveProgress);
        switch (progress.getStatus()) {
            case NEW:
                progress = archiveService.importFromDB(archiveProgress);
                break;
            case DOWNLOADED:
                progress = archiveService.transformRawData(archiveProgress);
                break;
            case TRANSFORMED:
                progress = archiveService.exportToDB(archiveProgress);
                break;
            case UPLOADED:
                progress = archiveService.finish(archiveProgress);
                break;
            default:
                log.warn(String.format("Illegal starting status %s for progress %s",
                        archiveProgress.getStatus(), archiveProgress.getRootOperationUID()));
        }

        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(archiveProgress);
        } else {
            logJobSucceed(archiveProgress);
        }
    }

    private ArchiveProgress retryJob(ArchiveProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            ProgressStatus resumeStatus;
            switch (progress.getStatusBeforeFailed()) {
                case NEW:
                case DOWNLOADING:
                    resumeStatus = ProgressStatus.NEW;
                    break;
                case DOWNLOADED:
                case TRANSFORMING:
                    resumeStatus = ProgressStatus.DOWNLOADED;
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

    public void kickOffNewProgress() {
        DateRange dateRange = archiveService.determineNewJobDateRange();
        log.info("Auto-determined date range is: " + dateRange);
        if (dateRange.getDurationInMilliSec() >= TimeUnit.DAYS.toMillis(1)) {
            archiveService.startNewProgress(dateRange.getStartDate(), dateRange.getEndDate(), jobSubmitter);
        } else {
            log.info("It is less than 24 hours since last archive.");
        }
    }

    private void logJobSucceed(ArchiveProgress progress) {
        log.info("Refreshing " + progress.getSourceName() + " finished for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }

    private void logJobFailed(ArchiveProgress progress) {
        log.error("Refreshing " + progress.getSourceName() + " failed for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }
}
