package com.latticeengines.propdata.collection.job.impl;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.job.ArchiveJobService;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;

@DisallowConcurrentExecution
@Component("featureArchiveJobService")
public class FeatureArchiveJobServiceImpl extends AbstractArchiveJobServiceImpl<FeatureArchiveProgress>
        implements ArchiveJobService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private FeatureArchiveService featureArchiveService;

    private boolean quartzEnabled = false;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (quartzEnabled) {
            setArchiveService(featureArchiveService);
            try {
                super.executeInternal(context);
            } catch (Exception e) {
                log.error(getProgressClass().getSimpleName() + "Failed.", e);
                throw new JobExecutionException(e);
            }
        }
    }

    @Override
    FeatureArchiveService getArchiveService() { return featureArchiveService; }

    @Override
    Logger getLogger() { return log; }

    @Override
    Class<FeatureArchiveProgress> getProgressClass() { return FeatureArchiveProgress.class; }

    // set job data as map
    public void setFeatureArchiveService(FeatureArchiveService featureArchiveService) {
        this.featureArchiveService = featureArchiveService;
    }

    public void setQuartzEnabled(boolean quartzEnabled) { this.quartzEnabled = quartzEnabled; }

}
