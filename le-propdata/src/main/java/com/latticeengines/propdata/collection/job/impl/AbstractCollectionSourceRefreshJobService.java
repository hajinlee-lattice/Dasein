package com.latticeengines.propdata.collection.job.impl;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.job.RefreshJobService;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.util.DateRange;

public abstract class AbstractCollectionSourceRefreshJobService extends QuartzJobBean implements RefreshJobService {

    private ArchiveService archiveService;
    protected String jobSubmitter = "Quartz";

    private static final int jobExpirationHours = 48; // expire a job after 48 hour
    private static final long jobExpirationMilliSeconds = TimeUnit.HOURS.toMillis(jobExpirationHours);

    abstract ArchiveService getArchiveService();
    abstract Log getLog();
    abstract CollectionSource getSource();

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        int t;
        Log log = getLog();
        for (t = 0; t < 100; t++) {
            try {
                if (tryExecuteInternal()) break;
            } catch (Exception e) {
                log.error("An archive job failed.");
            }
        }
        if (t == 100) {
            log.fatal(String.format("A single job executed has retried %d times!", t));
        }
    }

    @Override
    public void archivePeriod(DateRange period) {
        ArchiveProgress progress = archiveService.startNewProgress(period.getStartDate(), period.getEndDate(),
                jobSubmitter);
        proceedProgress(progress);
    }

    @Override
    public void retryJob(ArchiveProgress progress) {
        Log log = getLog();
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
        proceedProgress(progress);
    }

    @Override
    public void setJobSubmitter(String jobSubmitter) {
        this.jobSubmitter = jobSubmitter;
    }

    @Override
    public void setAutowiredArchiveService() {
        setArchiveService(getArchiveService());
    }

    private boolean tryExecuteInternal() throws InterruptedException {
        Log log = getLog();

        ArchiveProgress progress = archiveService.findRunningJob();
        if (progress != null) {
            log.info("There is a running " + progress);

            Date expireDate = new Date(System.currentTimeMillis() - jobExpirationMilliSeconds);
            Date lastUpdated = progress.getLatestStatusUpdate();
            if (lastUpdated.before(expireDate)) {
                log.fatal(String.format(
                        "This progress has been hanging for more than %d hours: %s", jobExpirationHours, progress));
            }
            return false;
        }

        progress = archiveService.findJobToRetry();
        if (progress == null) {
            log.info("There is nothing to retry for archiving " + getSource().getSourceName());
            DateRange dateRange = archiveService.determineNewJobDateRange();
            log.info("Auto-determine date range to be: " + dateRange);
            if (dateRange.getDurationInMilliSec() >= TimeUnit.DAYS.toMillis(7)) {
                progress = archiveService.startNewProgress(dateRange.getStartDate(), dateRange.getEndDate(),
                        jobSubmitter);
                proceedProgress(progress);
            } else {
                log.info("It is less than a week since last archive of " + getSource().getSourceName());
            }
            return true;
        } else {
            retryJob(progress);
        }

        Thread.sleep(10000L);

        return false;
    }

    protected void proceedProgress(ArchiveProgress progress) {
        switch (progress.getStatus()) {
            case NEW: progress = archiveService.importFromDB(progress);
            case DOWNLOADED: progress = archiveService.transformRawData(progress);
            case TRANSFORMED: progress = archiveService.exportToDB(progress);
            default: getLog().warn(String.format("Illegal starting status %s for progress %s",
                    progress.getStatus(), progress.getRootOperationUID()));
        }

        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            logJobFailed(progress);
        } else {
            logJobSucceed(progress);
        }
    }

    public void setArchiveService(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    private void logJobSucceed(ArchiveProgress progress) {
        Log log = getLog();
        log.info("Refreshing " + getSource().getSourceName() + " finished for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }
    private void logJobFailed(ArchiveProgress progress) {
        Log log = getLog();
        log.error("Refreshing " + getSource().getSourceName() + " failed for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }

}
