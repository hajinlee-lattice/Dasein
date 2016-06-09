package com.latticeengines.propdata.job;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("refreshHeartBeat")
public class RefreshHeartBeatJobBean implements QuartzJobBean {

    @Autowired
    private ProgressOrchestrator orchestrator;

    @Autowired
    private PropDataScheduler scheduler;

    @Override
    public Callable<Boolean> getCallable() {
        RefreshHeartBeatCallable.Builder builder = new RefreshHeartBeatCallable.Builder();
        builder.orchestrator(orchestrator).scheduler(scheduler);
        return new RefreshHeartBeatCallable(builder);
    }

}
