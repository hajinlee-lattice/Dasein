package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.Map;
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

    @Value("${dataflowapi.spark.driver.cores}")
    private String driverCores;

    @Value("${dataflowapi.spark.driver.mem}")
    private String driverMem;

    @Value("${dataflowapi.spark.executor.cores}")
    private String executorCores;

    @Value("${dataflowapi.spark.executor.mem}")
    private String executorMem;

    @Value("${dataflowapi.spark.max.executors}")
    private String maxExecutors;

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
            session = sessionService.startSession(livyHost, jobName, getSparkConf());
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

    private Map<String, String> getSparkConf() {
        Map<String, String> conf = new HashMap<>();
        conf.put("spark.driver.cores", driverCores);
        conf.put("spark.driver.memory", driverMem);
        conf.put("spark.executor.cores", executorCores);
        conf.put("spark.executor.memory", executorMem);
        conf.put("spark.dynamicAllocation.maxExecutors", maxExecutors);
        return conf;
    }

}
