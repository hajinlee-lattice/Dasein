package com.latticeengines.job.scheduler;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.collection.service.impl.RefreshExecutor;
import com.latticeengines.datacloud.etl.service.ServiceFlowsZkConfigService;

public class RefreshScheduler extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(RefreshScheduler.class);

    private RefreshService refreshService;
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;
    private boolean dryrun;

    private RefreshExecutor getExecutor() {
        return new RefreshExecutor(refreshService);
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Instantiated a RefreshScheduler fro service " + refreshService.getSource().getSourceName());
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (serviceFlowsZkConfigService.refreshJobEnabled(refreshService.getSource())) {
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

    public void setServiceFlowsZkConfigService(ServiceFlowsZkConfigService serviceFlowsZkConfigService) {
        this.serviceFlowsZkConfigService = serviceFlowsZkConfigService;
    }

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

}
