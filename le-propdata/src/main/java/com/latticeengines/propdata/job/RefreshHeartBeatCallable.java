package com.latticeengines.propdata.job;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.propdata.engine.transformation.TransformationProgressOrchestrator;
import com.latticeengines.proxy.exposed.propdata.IngestionProxy;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;

public class RefreshHeartBeatCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(RefreshHeartBeatCallable.class);

    private ProgressOrchestrator orchestrator;
    private TransformationProgressOrchestrator transformationOrchestrator;
    private PropDataScheduler scheduler;
    private PublicationProxy publicationProxy;
    @SuppressWarnings("unused")
    private IngestionProxy ingestionProxy;

    public RefreshHeartBeatCallable(Builder builder) {
        this.orchestrator = builder.getOrchestrator();
        this.transformationOrchestrator = builder.getTransformationOrchestrator();
        this.scheduler = builder.getScheduler();
        this.publicationProxy = builder.getPublicationProxy();
        this.ingestionProxy = builder.getIngestionProxy();
    }

    @Override
    public Boolean call() throws Exception {
        orchestrator.executeRefresh();
        transformationOrchestrator.executeRefresh();
        scheduler.reschedule();

        log.debug(this.getClass().getSimpleName() + " invoking publication proxy scan.");
        publicationProxy.scan("");
        return true;
    }

    public static class Builder {

        private ProgressOrchestrator orchestrator;
        private TransformationProgressOrchestrator transformationOrchestrator;
        private PropDataScheduler scheduler;
        private PublicationProxy publicationProxy;
        private IngestionProxy ingestionProxy;

        public Builder() {

        }

        public Builder orchestrator(ProgressOrchestrator orchestrator) {
            this.orchestrator = orchestrator;
            return this;
        }

        public Builder transformationOrchestrator(
                TransformationProgressOrchestrator transformationOrchestrator) {
            this.transformationOrchestrator = transformationOrchestrator;
            return this;
        }

        public Builder scheduler(PropDataScheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public Builder publicationProxy(PublicationProxy publicationProxy) {
            this.publicationProxy = publicationProxy;
            return this;
        }

        public Builder ingestionProxy(IngestionProxy ingestionProxy) {
            this.ingestionProxy = ingestionProxy;
            return this;
        }

        public ProgressOrchestrator getOrchestrator() {
            return orchestrator;
        }

        public TransformationProgressOrchestrator getTransformationOrchestrator() {
            return transformationOrchestrator;
        }

        public PropDataScheduler getScheduler() {
            return scheduler;
        }

        public PublicationProxy getPublicationProxy() {
            return publicationProxy;
        }

        public IngestionProxy getIngestionProxy() {
            return ingestionProxy;
        }
    }

}
