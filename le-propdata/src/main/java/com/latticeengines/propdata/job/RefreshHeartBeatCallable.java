package com.latticeengines.propdata.job;

import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;

public class RefreshHeartBeatCallable implements Callable<Boolean> {

    private static final Log log = LogFactory.getLog(RefreshHeartBeatCallable.class);

    private ProgressOrchestrator orchestrator;
    private PropDataScheduler scheduler;

    public RefreshHeartBeatCallable(Builder builder) {
        this.orchestrator = builder.getOrchestrator();
        this.scheduler = builder.getScheduler();
    }

    @Override
    public Boolean call() throws Exception {
        orchestrator.executeRefresh();
        scheduler.reschedule();
        return true;
    }

    public static class Builder {

        private ProgressOrchestrator orchestrator;
        private PropDataScheduler scheduler;

        public Builder() {
        }

        public Builder orchestrator(ProgressOrchestrator orchestrator) {
            this.orchestrator = orchestrator;
            return this;
        }

        public Builder scheduler(PropDataScheduler scheduler) {
            this.scheduler = scheduler;
            return this;
        }

        public ProgressOrchestrator getOrchestrator() {
            return orchestrator;
        }

        public PropDataScheduler getScheduler() {
            return scheduler;
        }
    }

}
