package com.latticeengines.job.scheduler;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.collection.service.impl.RefreshExecutor;
import com.latticeengines.datacloud.etl.service.ServiceFlowsZkConfigService;

public class RefreshScheduler extends QuartzJobBean {

    private static final Logger log = LoggerFactory.getLogger(RefreshScheduler.class);

    private RefreshService service;
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;
    private boolean dryrun;

    private RefreshExecutor getExecutor() {
        return new RefreshExecutor(service);
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Instantiated a RefreshScheduler for service " + service.getSource().getSourceName());
    }

    @Override
    protected void executeInternal(JobExecutionContext context) throws JobExecutionException {
        if (serviceFlowsZkConfigService.refreshJobEnabled(service.getSource())) {
            if (dryrun) {
                System.out.println(service.getClass().getSimpleName() + " triggered.");
            } else {
                getExecutor().kickOffNewProgress();
            }
        }
    }

    //==============================
    // for quartz detail bean
    //==============================
    public void setService(RefreshService service) {
        this.service = service;
    }

    public void setServiceFlowsZkConfigService(ServiceFlowsZkConfigService serviceFlowsZkConfigService) {
        this.serviceFlowsZkConfigService = serviceFlowsZkConfigService;
    }

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

}
