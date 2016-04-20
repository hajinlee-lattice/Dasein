package com.latticeengines.propdata.job;

import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.propdata.engine.transformation.TransformationProgressOrchestrator;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;

@DisallowConcurrentExecution
public class RefreshHeartBeat extends QuartzJobBean {

    private ProgressOrchestrator orchestrator;
    private TransformationProgressOrchestrator transformationOrchestrator;
    private PropDataScheduler scheduler;
    private PublicationProxy publicationProxy;

    @Override
    public void executeInternal(JobExecutionContext jobExecutionContext) throws JobExecutionException {
        orchestrator.executeRefresh();
        transformationOrchestrator.executeRefresh();
        scheduler.reschedule();

        publicationProxy.scan("");
    }

    // ==============================
    // for quartz detail bean
    // ==============================
    public void setOrchestrator(ProgressOrchestrator progressOrchestrator) {
        this.orchestrator = progressOrchestrator;
    }

    public void setTransformationOrchestrator(TransformationProgressOrchestrator transformationOrchestrator) {
        this.transformationOrchestrator = transformationOrchestrator;
    }

    public void setScheduler(PropDataScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setPublicationProxy(PublicationProxy publicationProxy) {
        this.publicationProxy = publicationProxy;
    }

}
