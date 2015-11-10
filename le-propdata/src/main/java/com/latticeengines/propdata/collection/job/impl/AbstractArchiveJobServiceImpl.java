package com.latticeengines.propdata.collection.job.impl;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.job.ArchiveJobService;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectionJobContext;
import com.latticeengines.propdata.collection.util.DateRange;

public abstract class AbstractArchiveJobServiceImpl<P extends ArchiveProgressBase> extends QuartzJobBean
        implements ArchiveJobService {

    private ArchiveService archiveService;
    private String jobSubmitter = "Quartz";

    private static final int jobExpirationHours = 48; // expire a job after 48 hour
    private static final long jobExpirationMilliSeconds = TimeUnit.HOURS.toMillis(jobExpirationHours);
    private static final Marker fatal = MarkerFactory.getMarker("FATAL");

    abstract ArchiveService getArchiveService();
    abstract Logger getLogger();
    abstract Class<P> getProgressClass();

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        int t;
        for (t = 0; t < 100; t++) { if (tryExecuteInternal()) break; }
        if (t == 100) {
            Logger log = getLogger();
            log.error(fatal, String.format("A single job executed has retried %d times!", t));
        }
    }

    @Override
    public void archivePeriod(DateRange period) {
        CollectionJobContext context = archiveService.startNewProgress(period.getStartDate(), period.getEndDate(),
                jobSubmitter);
        proceedProgress(context);
    }

    @Override
    public void setJobSubmitter(String jobSubmitter) {
        this.jobSubmitter = jobSubmitter;
    }

    @Override
    public void setAutowiredArchiveService() {
        setArchiveService(getArchiveService());
    }

    private boolean tryExecuteInternal() throws JobExecutionException {
        Logger log = getLogger();

        CollectionJobContext jobCtx = archiveService.findRunningJob();
        if (!CollectionJobContext.NULL.equals(jobCtx)) {
            P progress = jobCtx.getProperty(CollectionJobContext.PROGRESS_KEY, getProgressClass());
            log.info("There is a running " + progress);

            Date expireDate = new Date(System.currentTimeMillis() - jobExpirationMilliSeconds);
            Date lastUpdated = progress.getLatestStatusUpdate();
            if (lastUpdated.before(expireDate)) {
                log.error(fatal, String.format(
                        "This progress has been hanging for more than %d hours: %s", jobExpirationHours, progress));
            }
            return false;
        }

        jobCtx = archiveService.findJobToRetry();
        if (CollectionJobContext.NULL.equals(jobCtx)) {
            log.info("There is nothing to retry for " + getProgressClass().getSimpleName());
            DateRange dateRange = archiveService.determineNewJobDateRange();
            log.info("Auto-determine date range to be: " + dateRange);
            if (dateRange.getDurationInMilliSec() >= TimeUnit.DAYS.toMillis(7)) {
                jobCtx = archiveService.startNewProgress(dateRange.getStartDate(), dateRange.getEndDate(),
                        jobSubmitter);
                proceedProgress(jobCtx);
            } else {
                log.info("It is less than a week since last run of " + getProgressClass().getSimpleName());
            }
            return true;
        } else {
            P progress = jobCtx.getProperty(CollectionJobContext.PROGRESS_KEY, getProgressClass());
            log.info("Found a job to retry: " + progress);
            proceedProgress(jobCtx);
        }

        try {
            Thread.sleep(10000L);
        } catch (InterruptedException e) {
            throw new JobExecutionException(e);
        }

        return false;
    }

    private void proceedProgress(CollectionJobContext context) {
        context = archiveService.importFromDB(context);
        context = archiveService.transformRawData(context);
        context = archiveService.exportToDB(context);

        P progress = context.getProperty(CollectionJobContext.PROGRESS_KEY, getProgressClass());
        if (progress.getStatus().equals(ArchiveProgressStatus.FAILED)) {
            logJobFailed(progress);
        } else {
            logJobSucceed(progress);
        }
    }

    public void setArchiveService(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    private void logJobSucceed(P progress) {
        Logger log = getLogger();
        log.info(getProgressClass().getSimpleName() + " finished for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }
    private void logJobFailed(P progress) {
        Logger log = getLogger();
        log.error(getProgressClass().getSimpleName() + " failed for period " +
                new DateRange(progress.getStartDate(), progress.getEndDate()) +
                " RootOperationUID=" + progress.getRootOperationUID());
    }

}
