package com.latticeengines.domain.exposed.spark;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LivyConfigurer {

    private static final Logger log = LoggerFactory.getLogger(LivyConfigurer.class);

    private int driverCores = -1;
    private String driverMem;
    private int executorCores = -1;
    private String executorMem;
    private int maxExecutors = -1;
    private int minExecutors = -1;

    public LivyConfigurer withDriverMem(String driverMem) {
        this.driverMem = driverMem;
        return this;
    }

    public LivyConfigurer withDriverCores(int driverCores) {
        this.driverCores = driverCores;
        return this;
    }

    public LivyConfigurer withExecutorMem(String executorMem) {
        this.executorMem = executorMem;
        return this;
    }

    public LivyConfigurer withExecutorCores(int executorCores) {
        this.executorCores = executorCores;
        return this;
    }

    public LivyConfigurer withMinExecutors(int minExecutors) {
        this.minExecutors = minExecutors;
        return this;
    }

    public LivyConfigurer withMaxExecutors(int maxExecutors) {
        this.maxExecutors = maxExecutors;
        return this;
    }

    public Map<String, Object> getLivyConf(LivyScalingConfig livySessionConfig) {
        int scalingMultiplier = livySessionConfig.scalingMultiplier;
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
            int newCores = 2 * executorCores;
            log.info("Scale up executor cores to " + newCores + " memory to " + newMem //
                    + " based on scalingFactor=" + scalingMultiplier);
            conf.put("executorCores", newCores);
            conf.put("executorMemory", newMem);
        }
        return conf;
    }

    public Map<String, String> getSparkConf(LivyScalingConfig livySessionConfig) {
        int scalingMultiplier = livySessionConfig.scalingMultiplier;
        int partitionMultiplier = scalingMultiplier > 1 ? Math.max(1, livySessionConfig.partitionMultiplier) : 1;

        scalingMultiplier = Math.max(scalingMultiplier - 1, 1);
        Map<String, String> conf = new HashMap<>();
        int minExe = minExecutors * scalingMultiplier;
        int maxExe = maxExecutors * scalingMultiplier;
        conf.put("spark.executor.instances", "1");
        conf.put("spark.dynamicAllocation.initialExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(maxExe));

        int partitions = minExe * executorCores * partitionMultiplier;
        conf.put("spark.default.parallelism", String.valueOf(partitions));
        conf.put("spark.sql.shuffle.partitions", String.valueOf(partitions));
        return conf;
    }

}
