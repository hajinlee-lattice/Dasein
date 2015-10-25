package com.latticeengines.propdata.collection.job.impl;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
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

    abstract ArchiveService getArchiveService();
    abstract Logger getLogger();
    abstract Class<P> getProgressClass();

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        CollectionJobContext jobCtx = archiveService.findRetriableArchiveJob();
        if (CollectionJobContext.NULL.equals(jobCtx)) {

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
