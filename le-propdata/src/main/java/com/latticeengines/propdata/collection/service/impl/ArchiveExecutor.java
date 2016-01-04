package com.latticeengines.propdata.collection.service.impl;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.collection.service.RefreshJobExecutor;
import com.latticeengines.propdata.collection.util.DateRange;

public class ArchiveExecutor implements RefreshJobExecutor {

    private final String jobSubmitter;

    private final ArchiveService archiveService;
    private static final Log log = LogFactory.getLog(ArchiveExecutor.class);

    public ArchiveExecutor(ArchiveService archiveService) {
        this.archiveService = archiveService;
        this.jobSubmitter = archiveService.getClass().getSimpleName();
    }

    @Override
    public synchronized void proceedProgress(Progress progress) {
        ArchiveProgress archiveProgress = (ArchiveProgress) progress;
        archiveProgress = retryJob(archiveProgress);
        switch (archiveProgress.getStatus()) {
            case NEW:
                archiveProgress = archiveService.importFromDB(archiveProgress);
                break;
            case DOWNLOADED:
                archiveProgress = archiveService.finish(archiveProgress);
                break;
            default:
                log.warn(String.format("Illegal starting status %s for progress %s",
                        archiveProgress.getStatus(), archiveProgress.getRootOperationUID()));
        }

        if (archiveProgress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(archiveProgress);
        } else {
            logJobSucceed(archiveProgress);
        }
    }

    @Override
    public synchronized void print() {
        try {
            String uuid = UUID.randomUUID().toString();
            Random random = new Random();
            System.out.println(archiveService + " - " + uuid + " start.");
            Thread.sleep(random.nextInt(3000));
            System.out.println(archiveService + " - " + uuid + " finished.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private ArchiveProgress retryJob(ArchiveProgress progress) {
        if (ProgressStatus.FAILED.equals(progress.getStatus())) {
            log.info("Found a job to retry: " + progress);
            progress.setStatus(ProgressStatus.NEW);
        }
        return progress;
    }

    public void kickOffNewProgress() {
        if (archiveService instanceof CollectedArchiveService) {
            DateRange dateRange = ((CollectedArchiveService) archiveService).determineNewJobDateRange();
            log.info("Auto-determined date range is: " + dateRange);
            if (dateRange.getDurationInMilliSec() >= TimeUnit.DAYS.toMillis(1)) {
                archiveService.startNewProgress(dateRange.getStartDate(), dateRange.getEndDate(), jobSubmitter);
            } else {
                log.info("It is less than 24 hours since last archive.");
            }
        } else {
            log.info("Archiving a snapshot of bulk source.");
            archiveService.startNewProgress(null, null, jobSubmitter);
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
