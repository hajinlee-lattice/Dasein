package com.latticeengines.propdata.job;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.collection.service.impl.RefreshExecutor;

public class RefreshScheduler extends QuartzJobBean {

    private RefreshService refreshService;
    private ZkConfigurationService zkConfigurationService;
    private boolean dryrun;

    private RefreshExecutor getExecutor() {
        return new RefreshExecutor(refreshService);
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (zkConfigurationService.refreshJobEnabled(refreshService.getSource())) {
            if (dryrun) {
                System.out.println(refreshService.getClass().getSimpleName() + " triggered.");
            } else {
                getExecutor().kickOffNewProgress();
            }
        }
    }

    //==============================
    // for quartz detail bean
    //==============================
    public void setRefreshService(RefreshService refreshService) {
        this.refreshService = refreshService;
    }

    public void setZkConfigurationService(ZkConfigurationService zkConfigurationService) {
        this.zkConfigurationService = zkConfigurationService;
    }

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

}
