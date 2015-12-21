package com.latticeengines.propdata.job.impl;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.impl.CollectionRefreshExecutor;

public class CollectionRefreshScheduler extends QuartzJobBean {

    private ArchiveService archiveService;

    private CollectionRefreshExecutor getExecutor() {
        return new CollectionRefreshExecutor(archiveService);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        getExecutor().kickOffNewProgress();
    }

    public void setArchiveService(ArchiveService archiveService) {
        this.archiveService = archiveService;
    }

}
