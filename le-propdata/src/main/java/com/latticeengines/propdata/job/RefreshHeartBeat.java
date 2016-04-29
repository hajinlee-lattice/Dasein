package com.latticeengines.propdata.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.quartz.DisallowConcurrentExecution;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.springframework.scheduling.quartz.QuartzJobBean;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.proxy.exposed.propdata.IngestionProxy;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;
import com.latticeengines.proxy.exposed.propdata.TransformationProxy;

@DisallowConcurrentExecution
public class RefreshHeartBeat extends QuartzJobBean {

    private static final Log log = LogFactory.getLog(RefreshHeartBeat.class);

    private ProgressOrchestrator orchestrator;
    private TransformationProxy transformationProxy;
    private PropDataScheduler scheduler;
    private PublicationProxy publicationProxy;
    private IngestionProxy ingestionProxy;

    @Override
    public void executeInternal(JobExecutionContext jobExecutionContext)
            throws JobExecutionException {

        orchestrator.executeRefresh();
        scheduler.reschedule();

        log.debug(this.getClass().getSimpleName() + " invoking publication proxy scan.");
        publicationProxy.scan("");

        log.debug(this.getClass().getSimpleName() + " invoking transformation proxy scan.");
        transformationProxy.scan("");

        // log.debug(this.getClass().getSimpleName() + " invoking ingestion
        // proxy scan.");
        // ingestionProxy.scan("FunctionalBomboraFireHose");
    }

    // ==============================
    // for quartz detail bean
    // ==============================
    public void setOrchestrator(ProgressOrchestrator progressOrchestrator) {
        this.orchestrator = progressOrchestrator;
    }

    public void setTransformationProxy(TransformationProxy transformationProxy) {
        this.transformationProxy = transformationProxy;
    }

    public void setScheduler(PropDataScheduler scheduler) {
        this.scheduler = scheduler;
    }

    public void setPublicationProxy(PublicationProxy publicationProxy) {
        this.publicationProxy = publicationProxy;
    }

    public void setIngestionProxy(IngestionProxy ingestionProxy) {
        this.ingestionProxy = ingestionProxy;
    }

}
