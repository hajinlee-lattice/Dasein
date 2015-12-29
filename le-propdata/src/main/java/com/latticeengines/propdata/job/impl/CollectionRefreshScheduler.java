package com.latticeengines.propdata.job.impl;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.collection.service.impl.ArchiveExecutor;

public class CollectionRefreshScheduler extends QuartzJobBean {

    private CollectedArchiveService collectedArchiveService;

    private ArchiveExecutor getExecutor() {
        return new ArchiveExecutor(collectedArchiveService, null);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        getExecutor().kickOffNewProgress();
    }

    public void setCollectedArchiveService(CollectedArchiveService collectedArchiveService) {
        this.collectedArchiveService = collectedArchiveService;
    }

}
