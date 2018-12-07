package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.spark.exposed.service.LivySessionService;

@Component
public class LivySessionHolder {

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    private final AtomicReference<LivySession> livySessionHolder = new AtomicReference<>(null);

    public LivySession getOrCreateLivySession(String jobName) {
        LivySession session = livySessionHolder.get();
        if (session == null) {
            String livyHost;
            if (Boolean.TRUE.equals(useEmr)) {
                livyHost = emrCacheService.getLivyUrl();
            } else {
                livyHost = "http://localhost:8998";
            }
            Runtime.getRuntime().addShutdownHook(new Thread(this::killSession));
            if (StringUtils.isBlank(jobName)) {
                jobName = "Workflow";
            }
            session = sessionService.startSession(livyHost,jobName);
            livySessionHolder.set(session);
        }
        return session;
    }

    public void killSession() {
        LivySession session = livySessionHolder.get();
        if (session != null) {
            livySessionHolder.set(null);
            sessionService.stopSession(session);
        }
    }

}
