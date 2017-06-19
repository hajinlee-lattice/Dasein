package com.latticeengines.job.scheduler;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.service.impl.ProgressOrchestrator;

@Component("refreshHeartBeat")
@DisallowConcurrentExecution
public class RefreshHeartBeat extends QuartzJobBean {

    private ProgressOrchestrator orchestrator;
    private PropDataScheduler scheduler;

    @Override
    public void executeInternal(JobExecutionContext jobExecutionContext)
            throws JobExecutionException {
        orchestrator.executeRefresh();
        scheduler.reschedule();
    }

    // ==============================
    // for quartz detail bean
    // ==============================
    public void setOrchestrator(ProgressOrchestrator progressOrchestrator) {
        this.orchestrator = progressOrchestrator;
    }

    public void setScheduler(PropDataScheduler scheduler) {
        this.scheduler = scheduler;
    }

}
