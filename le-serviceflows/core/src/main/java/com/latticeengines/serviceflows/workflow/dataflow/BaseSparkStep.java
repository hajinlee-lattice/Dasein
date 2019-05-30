package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public abstract class BaseSparkStep<S extends BaseStepConfiguration> extends BaseWorkflowStep<S> {

    private static final Logger log = LoggerFactory.getLogger(BaseSparkStep.class);

    @Inject
    private LivySessionManager livySessionManager;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Resource(name = "distCpConfiguration")
    protected Configuration distCpConfiguration;

    @Resource(name = "yarnConfiguration")
    protected Configuration yarnConfiguration;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

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
        if (scalingMultiplier > 1) {
            // when scaling factor > 1, we eagerly want more executors
            minExe *= 2;
        }
        int maxExe = maxExecutors * scalingMultiplier;
        conf.put("spark.executor.instances", "1");
        conf.put("spark.dynamicAllocation.initialExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.minExecutors", String.valueOf(minExe));
        conf.put("spark.dynamicAllocation.maxExecutors", String.valueOf(maxExe));
        int partitions = Math.max(maxExe * executorCores * 2, 200);
        conf.put("spark.default.parallelism", String.valueOf(partitions));
        conf.put("spark.sql.shuffle.partitions", String.valueOf(partitions));
        return conf;
    }

    protected void exportToS3AndAddToContext(Table table, String contextKey) {
        String tableName = table.getName();
        boolean shouldSkip = getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class);
        if (!shouldSkip) {
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            String queueName = LedpQueueAssigner.getEaiQueueNameForSubmission();
            queueName = LedpQueueAssigner.overwriteQueueAssignment(queueName, emrEnvService.getYarnQueueScheme());
            ImportExportRequest batchStoreRequest = ImportExportRequest.exportAtlasTable( //
                    customerSpace.toString(), table, //
                    pathBuilder, s3Bucket, podId, //
                    yarnConfiguration, //
                    fileStatus -> true);
            if (batchStoreRequest == null) {
                throw new IllegalArgumentException("Cannot construct proper export request for " + tableName);
            }
            HdfsS3ImporterExporter exporter = new HdfsS3ImporterExporter( //
                    customerSpace.toString(), distCpConfiguration, queueName, dataUnitProxy, batchStoreRequest);
            exporter.run();
        } else {
            log.info("Skip publish " + contextKey + " (" + tableName + ") to S3.");
        }
        putStringValueInContext(contextKey, tableName);
    }

}
