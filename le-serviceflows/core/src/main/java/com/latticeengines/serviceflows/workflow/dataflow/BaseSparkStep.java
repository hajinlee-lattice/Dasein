package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

public abstract class BaseSparkStep<S extends BaseStepConfiguration> extends BaseWorkflowStep<S> {

    @Inject
    private LivySessionManager livySessionManager;

    @Value("${camille.zk.pod.id}")
    protected String podId;

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

    protected CustomerSpace customerSpace;
    private int scalingMultiplier;

    LivySession createLivySession(String jobName) {
        return livySessionManager.createLivySession(jobName, getLivyConf(), getSparkConf());
    }

    void killLivySession() {
        livySessionManager.killSession();
    }

    void computeScalingMultiplier(List<DataUnit> inputs) {
        long totalInputCount = inputs.stream().mapToLong(du -> {
            if (du instanceof HdfsDataUnit) {
                return du.getCount() == null ? 0L : du.getCount();
            } else {
                return 0L;
            }
        }).sum();
        scalingMultiplier = ScalingUtils.getMultiplier(totalInputCount);
        log.info("Set scalingMultiplier=" + scalingMultiplier + " based on totalInputCount=" + totalInputCount);
    }

    protected Table toTable(String tableName, String primaryKey, HdfsDataUnit jobTarget) {
        return SparkUtils.hdfsUnitToTable(tableName, primaryKey, jobTarget, yarnConfiguration, podId, customerSpace);
    }

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        return toTable(tableName, null, jobTarget);
    }

    private Map<String, Object> getLivyConf() {
        Map<String, Object> conf = new HashMap<>();
        conf.put("driverCores", driverCores);
        conf.put("driverMemory", driverMem);
        conf.put("executorCores", executorCores);
        conf.put("executorMemory", executorMem);
        return conf;
    }

    private Map<String, String> getSparkConf() {
        Map<String, String> conf = new HashMap<>();
        int minExe = minExecutors * scalingMultiplier;
        int maxExe = maxExecutors * scalingMultiplier;
        conf.put("spark.executor.instances", String.valueOf(Math.max(minExe, 1)));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(maxExe));
        return conf;
    }

}
