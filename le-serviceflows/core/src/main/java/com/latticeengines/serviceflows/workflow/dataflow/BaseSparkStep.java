package com.latticeengines.serviceflows.workflow.dataflow;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.spark.LivyScalingConfig;
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

    @Inject
    private S3Service s3Service;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    protected CustomerSpace customerSpace;
    private int scalingMultiplier = 1;
    private int partitionMultiplier = 1;
    private String sparkMaxResultSize = null;

    protected LivySession createLivySession(String jobName) {
        Map<String, String> extraConf = null;
        if (sparkMaxResultSize != null) {
            extraConf = new HashMap<>();
            extraConf.put("spark.driver.maxResultSize", sparkMaxResultSize);
        }
        return livySessionManager.createLivySession(jobName, //
                new LivyScalingConfig(scalingMultiplier, partitionMultiplier), extraConf);
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

    protected Table dirToTable(String tableName, HdfsDataUnit jobTarget) {
        return dirToTable(tableName, null, jobTarget);
    }

    protected Table dirToTable(String tableName, String primaryKey, HdfsDataUnit jobTarget) {
        return SparkUtils.hdfsUnitDirToTable(tableName, primaryKey, jobTarget, yarnConfiguration, podId, customerSpace);
    }

    protected Table toTable(String tableName, String primaryKey, HdfsDataUnit jobTarget) {
        return SparkUtils.hdfsUnitToTable(tableName, primaryKey, jobTarget, yarnConfiguration, podId, customerSpace);
    }

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        return toTable(tableName, null, jobTarget);
    }

    protected boolean exportToS3(Table table) {
        String tableName = table.getName();
        boolean shouldSkip = Boolean.TRUE.equals(getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class));
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
        }
        return shouldSkip;
    }

    protected Map<String, String> exportToS3AndAddToContext(Map<String, Table> tables, String contextKey) {
        Map<String, String> tableNames = tables.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), entry.getValue().getName())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            boolean skipped = exportToS3(entry.getValue());
            if (skipped) {
                log.info("Skip publish {} ({}) to S3.", contextKey, tables.keySet());
                break;
            }
        }
        putObjectInContext(contextKey, tableNames);
        return tableNames;
    }

    protected void exportToS3AndAddToContext(Table table, String contextKey) {
        String tableName = table.getName();
        boolean skipped = exportToS3(table);
        if (skipped) {
            log.info("Skip publish " + contextKey + " (" + tableName + ") to S3.");
        }
        putStringValueInContext(contextKey, tableName);
    }

    protected void setPartitionMultiplier(int partitionMultiplier) {
        this.partitionMultiplier = partitionMultiplier;
        log.info("Adjust partitionMultiplier to {}", this.partitionMultiplier);
    }

    protected void setSparkMaxResultSize(String maxResultSize) {
        this.sparkMaxResultSize = maxResultSize;
        log.info("Adjust sparkMaxResultSize to " + this.sparkMaxResultSize);
    }

    protected String getFirstCsvFilePath(HdfsDataUnit dataUnit) {
        String outputDir = dataUnit.getPath();
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, outputDir, //
                    (HdfsUtils.HdfsFilenameFilter) filename -> //
                            filename.endsWith(".csv.gz") || filename.endsWith(".csv"));
            return files.get(0);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read " + outputDir);
        }
    }

    protected void copyToS3(String hdfsPath, String s3Path) throws IOException {
        copyToS3(hdfsPath, s3Path, null, null);
    }

    protected void copyToS3(String hdfsPath, String s3Path, String tag, String tagValue) throws IOException {
        log.info("Copy from " + hdfsPath + " to " + s3Path);
        long fileSize = HdfsUtils.getFileSize(yarnConfiguration, hdfsPath);
        RetryTemplate retry = RetryUtils.getRetryTemplate(10, //
                Collections.singleton(AmazonS3Exception.class), null);
        retry.execute(context -> {
            if (context.getRetryCount() > 0) {
                log.info(String.format("(Attempt=%d) Retry copying file from hdfs://%s to s3://%s/%s", //
                        context.getRetryCount() + 1, hdfsPath, s3Bucket, s3Path));
            }
            try (InputStream stream = HdfsUtils.getInputStream(yarnConfiguration, hdfsPath)) {
                s3Service.uploadInputStreamMultiPart(s3Bucket, s3Path, stream, fileSize);
                if (StringUtils.isNotBlank(tag) && StringUtils.isNotBlank(tagValue)) {
                    s3Service.addTagToObject(s3Bucket, s3Path, tag, tagValue);
                }
            }
            return true;
        });
    }
}
