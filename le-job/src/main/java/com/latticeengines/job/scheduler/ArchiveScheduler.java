package com.latticeengines.job.scheduler;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.datacloud.collection.service.ArchiveService;
import com.latticeengines.datacloud.collection.service.impl.ArchiveExecutor;
import com.latticeengines.datacloud.etl.service.ServiceFlowsZkConfigService;


public class ArchiveScheduler extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(ArchiveScheduler.class);

    private ArchiveService service;
    private ServiceFlowsZkConfigService serviceFlowsZkConfigService;
    private boolean dryrun;

    private ArchiveExecutor getExecutor() {
        return new ArchiveExecutor(service);
    }

    @PostConstruct
    public void postConstruct() {
        log.info("Instantiated an ArchiveScheduler for service " + service.getSource().getSourceName());
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
    public void setService(ArchiveService service) {
        this.service = service;
    }

    public void setServiceFlowsZkConfigService(ServiceFlowsZkConfigService serviceFlowsZkConfigService) {
        this.serviceFlowsZkConfigService = serviceFlowsZkConfigService;
    }

    public void setDryrun(boolean dryrun) { this.dryrun = dryrun; }

}
