package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.spark.exposed.service.LivySessionService;

@Component
public class LivySessionManager {

    private static final Logger log = LoggerFactory.getLogger(LivySessionManager.class);

    @Inject
    private LivySessionService sessionService;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

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

    LivySession createLivySession(String jobName, int scalingMultiplier) {
        LivySession session = livySessionHolder.get();
        if (session != null) {
            killSession();
        }
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
        session = sessionService.startSession(livyHost, jobName, //
                getLivyConf(scalingMultiplier), getSparkConf(scalingMultiplier));
        livySessionHolder.set(session);
        return session;
    }

    private Map<String, Object> getLivyConf(int scalingMultiplier) {
        Map<String, Object> conf = new HashMap<>();
        conf.put("driverCores", driverCores);
        conf.put("driverMemory", driverMem);
        conf.put("executorCores", executorCores);
        conf.put("executorMemory", executorMem);
        if (scalingMultiplier > 1) {
            // scale up first
            String unit = executorMem.substring(executorMem.length()-1);
            int val = Integer.parseInt(executorMem.replace(unit, ""));
            String newMem = String.format("%d%s", 2 * val, unit);
            log.info("Double executor memory to " + newMem + " based on scalingFactor=" + scalingMultiplier);
            conf.put("executorMemory", newMem);
        }
        return conf;
    }

    private Map<String, String> getSparkConf(int scalingMultiplier) {
        Map<String, String> conf = new HashMap<>();
        int minExe = minExecutors * scalingMultiplier;
        int maxExe = Math.max((int) (maxExecutors * scalingMultiplier * 0.5), minExe);
        conf.put("spark.executor.instances", "1");
        conf.put("spark.dynamicAllocation.initialExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(maxExe));
        int partitions = Math.max(maxExe * executorCores * 2, 200);
        conf.put("spark.default.parallelism", String.valueOf(partitions));
        conf.put("spark.sql.shuffle.partitions", String.valueOf(partitions));
        return conf;
    }

    void killSession() {
        LivySession session = livySessionHolder.get();
        if (session != null) {
            livySessionHolder.set(null);
            sessionService.stopSession(session);
        }
    }

}
