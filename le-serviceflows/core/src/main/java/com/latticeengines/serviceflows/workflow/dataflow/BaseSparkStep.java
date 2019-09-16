package com.latticeengines.serviceflows.workflow.dataflow;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.serviceflows.workflow.util.ScalingUtils;
import com.latticeengines.serviceflows.workflow.util.SparkUtils;
import com.latticeengines.spark.exposed.job.AbstractSparkJob;
import com.latticeengines.spark.exposed.service.SparkJobService;
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

    @Inject
    private SparkJobService sparkJobService;

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

    protected CustomerSpace customerSpace;
    private int scalingMultiplier = 1;
    private int partitionMultiplier = 1;

    protected LivySession createLivySession(String jobName) {
        return livySessionManager.createLivySession(jobName, //
                new LivySessionConfig(scalingMultiplier, partitionMultiplier));
    }

    protected void killLivySession() {
        livySessionManager.killSession();
    }

    protected void computeScalingMultiplier(List<DataUnit> inputs, int numberOfTargets) {
        double totalSizeInGb = inputs.stream().mapToDouble(du -> {
            if (du instanceof HdfsDataUnit) {
                String path = ((HdfsDataUnit) du).getPath();
                return ScalingUtils.getHdfsPathSizeInGb(yarnConfiguration, path);
            } else {
                return 0.0;
            }
        }).sum();
        scalingMultiplier = ScalingUtils.getMultiplier(totalSizeInGb * Math.max(1, numberOfTargets));
        log.info("Set scalingMultiplier=" + scalingMultiplier + " based on totalSize=" + totalSizeInGb //
                + " gb and numberOfTargets=" + numberOfTargets);
    }

    protected <C extends SparkJobConfig, J extends AbstractSparkJob<C>> //
    SparkJobResult runSparkJob(LivySession session, Class<J> jobClz, C jobConfig) {
        return sparkJobService.runJob(session, jobClz, jobConfig);
    }

    protected String getRandomWorkspace() {
        return PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
    }

    protected Table toTable(String tableName, String primaryKey, HdfsDataUnit jobTarget) {
        return SparkUtils.hdfsUnitToTable(tableName, primaryKey, jobTarget, yarnConfiguration, podId, customerSpace);
    }

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        return toTable(tableName, null, jobTarget);
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

    protected void setPartitionMultiplier(int partitionMultiplier) {
        this.partitionMultiplier = partitionMultiplier;
        log.info("Adjust partitionMultiplier to {}", this.partitionMultiplier);
    }

}
