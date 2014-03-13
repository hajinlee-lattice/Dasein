package com.latticeengines.dataplatform.service.impl;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.dataplatform.entitymanager.ModelEntityMgr;
import com.latticeengines.dataplatform.entitymanager.ThrottleConfigurationEntityMgr;
import com.latticeengines.dataplatform.service.JobService;


public class JobWatchdogServiceImpl extends QuartzJobBean {
    
    private JobService jobService;
    private ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr;
    private ModelEntityMgr modelEntityMgr;

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
    }
    
    private void computeJobsToThrottle() {
        
    }
    
    private void resubmitPreemptedJobs() {
        
    }

    public JobService getJobService() {
        return jobService;
    }

    public void setJobService(JobService jobService) {
        this.jobService = jobService;
    }

    public ThrottleConfigurationEntityMgr getThrottleConfigurationEntityMgr() {
        return throttleConfigurationEntityMgr;
    }

    public void setThrottleConfigurationEntityMgr(ThrottleConfigurationEntityMgr throttleConfigurationEntityMgr) {
        this.throttleConfigurationEntityMgr = throttleConfigurationEntityMgr;
    }

    public ModelEntityMgr getModelEntityMgr() {
        return modelEntityMgr;
    }

    public void setModelEntityMgr(ModelEntityMgr modelEntityMgr) {
        this.modelEntityMgr = modelEntityMgr;
    }

}
