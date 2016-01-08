package com.latticeengines.propdata.job;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.impl.ArchiveExecutor;

public class ArchiveScheduler extends QuartzJobBean {

    private ArchiveService archiveService;
    private boolean dryrun;

    private ArchiveExecutor getExecutor() {
        return new ArchiveExecutor(archiveService);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (dryrun) {
            System.out.println(archiveService.getClass().getSimpleName() + " triggered.");
        } else {
            getExecutor().kickOffNewProgress();
        }
    }

    //==============================
    // for quartz detail bean
    //==============================
    public void setArchiveService(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

}
