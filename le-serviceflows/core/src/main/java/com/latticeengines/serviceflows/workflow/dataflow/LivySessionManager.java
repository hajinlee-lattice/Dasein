package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.LivyConfigurer;
import com.latticeengines.domain.exposed.spark.LivyScalingConfig;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.spark.exposed.service.LivySessionService;

@Component
public class LivySessionManager {

    @Inject
    private LivySessionService sessionService;

    @Value("${dataflowapi.spark.driver.cores}")
    private int driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private int executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private int maxExecutors;

    @Value("${dataflowapi.spark.min.executors}")
    private int minExecutors;

    private final AtomicReference<LivySession> livySessionHolder = new AtomicReference<>(null);

    public LivySession createLivySession(String jobName, LivyScalingConfig livySessionConfig) {
        LivySession session = livySessionHolder.get();
        if (session != null) {
            killSession();
        }
        Runtime.getRuntime().addShutdownHook(new Thread(this::killSession));
        if (StringUtils.isBlank(jobName)) {
            jobName = "Workflow";
        }
        LivyConfigurer configurer = new LivyConfigurer() //
                .withDriverMem(driverMem).withDriverCores(driverCores) //
                .withExecutorMem(executorMem).withExecutorCores(executorCores) //
                .withMinExecutors(minExecutors).withMaxExecutors(maxExecutors);
        session = sessionService.startSession(jobName, //
                configurer.getLivyConf(livySessionConfig), configurer.getSparkConf(livySessionConfig));
        livySessionHolder.set(session);
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
