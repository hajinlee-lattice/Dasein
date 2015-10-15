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
import com.latticeengines.propdata.collection.util.LoggingUtils;

public abstract class AbstractArchiveJobServiceImpl<P extends ArchiveProgressBase> extends QuartzJobBean
        implements ArchiveJobService {

    private ArchiveService archiveService;
    private String jobSubmitter = "Quartz";

    abstract ArchiveService getArchiveService();
    abstract Logger getLogger();
    abstract Class<P> getProgressClass();

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    }

    @Override
    public void archivePeriod(DateRange period) {
        Logger log = getLogger();
        CollectionJobContext context = archiveService.startNewProgress(period.getStartDate(), period.getEndDate(),
                jobSubmitter);
        P progress = context.getProperty(CollectionJobContext.PROGRESS_KEY, getProgressClass());
        LoggingUtils.logInfo(log, progress, "Start Archiving process.");

        context = archiveService.importFromDB(context);
        context = archiveService.transformRawData(context);
        context = archiveService.exportToDB(context);

        progress = context.getProperty(CollectionJobContext.PROGRESS_KEY, getProgressClass());
        if (progress.getStatus().equals(ArchiveProgressStatus.FAILED)) {
            logJobFailed(progress);
        } else {
            logJobSucceed(progress);
        }
    }

    @Override
    public void setJobSubmitter(String jobSubmitter) {
        this.jobSubmitter = jobSubmitter;
    }

    @Override
    public void setAutowiredArchiveService() {
        setArchiveService(getArchiveService());
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
