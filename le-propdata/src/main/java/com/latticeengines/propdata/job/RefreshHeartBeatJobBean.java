package com.latticeengines.propdata.job;

import java.util.concurrent.Callable;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.service.impl.ProgressOrchestrator;
import com.latticeengines.propdata.engine.transformation.TransformationProgressOrchestrator;
import com.latticeengines.proxy.exposed.propdata.IngestionProxy;
import com.latticeengines.proxy.exposed.propdata.PublicationProxy;
import com.latticeengines.quartzclient.mbean.QuartzJobBean;

@Component("refreshHeartBeat")
public class RefreshHeartBeatJobBean implements QuartzJobBean {

    @Autowired
    private ProgressOrchestrator orchestrator;

    @Autowired
    private TransformationProgressOrchestrator transformationOrchestrator;

    @Autowired
    private PropDataScheduler scheduler;

    @Autowired
    private PublicationProxy publicationProxy;

    @Autowired
    private IngestionProxy ingestionProxy;

    @Override
    public Callable<Boolean> getCallable() {
        RefreshHeartBeatCallable.Builder builder = new RefreshHeartBeatCallable.Builder();
        builder.orchestrator(orchestrator)
                .transformationOrchestrator(transformationOrchestrator)
                .scheduler(scheduler)
                .publicationProxy(publicationProxy)
                .ingestionProxy(ingestionProxy);
        return new RefreshHeartBeatCallable(builder);
    }

}
