package com.latticeengines.serviceflows.workflow.dataflow;

import static com.latticeengines.domain.exposed.metadata.datastore.DataUnit.StorageType.S3;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.util.ReflectionUtils;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.latticeengines.aws.s3.S3Service;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.yarn.LedpQueueAssigner;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.metadata.datastore.S3DataUnit;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;
import com.latticeengines.domain.exposed.spark.LivyScalingConfig;
import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
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
    private List<String> workSpaces = new ArrayList<>();
    ExecutorService s3ExporterPool;

    @PreDestroy
    public void tearDown() {
        clearAllWorkspaces();
    }

    protected CustomerSpace parseCustomerSpace(S stepConfiguration) {
        if (stepConfiguration instanceof SparkJobStepConfiguration) {
            SparkJobStepConfiguration sparkJobStepConfiguration = (SparkJobStepConfiguration) stepConfiguration;
            return CustomerSpace.parse(sparkJobStepConfiguration.getCustomer());
        } else {
            Method method = ReflectionUtils.findMethod(stepConfiguration.getClass(), "getCustomerSpace");
            if (method != null) {
                if (CustomerSpace.class.equals(method.getReturnType())) {
                    return (CustomerSpace) ReflectionUtils.invokeMethod(method, stepConfiguration);
                } else if (String.class.equals(method.getReturnType())) {
                    String customerSpaceStr = (String) ReflectionUtils.invokeMethod(method, stepConfiguration);
                    return CustomerSpace.parse(customerSpaceStr);
                }
            }
            throw new UnsupportedOperationException("Do not know how to parse customer space from a " //
                    + stepConfiguration.getClass().getCanonicalName());
        }
    }

    protected DataUnit getDataUnit(boolean queryDataUnit, CustomerSpace customerSpace, String unitName) {
        if (unitName == null) {
            return null;
        } else if (queryDataUnit) {
            S3DataUnit s3DataUnit = (S3DataUnit) dataUnitProxy.getByNameAndType(customerSpace.getTenantId(), unitName, DataUnit.StorageType.S3);
            if (s3DataUnit == null) {
                throw new RuntimeException("S3 data unit " + unitName + " for customer " + customerSpace.getTenantId() + " does not exists.");
            }
            return s3DataUnit;
        } else {
            Table table = metadataProxy.getTable(customerSpace.toString(), unitName);
            if (table == null) {
                throw new RuntimeException("Table " + unitName + " for customer " + CustomerSpace.shortenCustomerSpace(customerSpace.toString()) //
                        + " does not exists.");
            }
            return table.toHdfsDataUnit(unitName);
        }
    }


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

    protected <C extends SparkJobConfig, J extends AbstractSparkJob<C>> //
    SparkJobResult runSparkJob(Class<J> jobClz, C jobConfig) {
        if (customerSpace == null) {
            customerSpace = parseCustomerSpace(configuration);
        }
        if (StringUtils.isBlank(jobConfig.getWorkspace())) {
            jobConfig.setWorkspace(getRandomWorkspace());
        }
        String configStr = JsonUtils.serialize(jobConfig);
        if (configStr.length() > 1000) {
            configStr = "long string";
        }
        log.info("Run spark job " + jobClz.getSimpleName() + " with configuration: " + configStr);
        computeScalingMultiplier(jobConfig.getInput(), jobConfig.getNumTargets());
        try {
            RetryTemplate retry = RetryUtils.getRetryTemplate(3);
            return retry.execute(context -> {
                if (context.getRetryCount() > 0) {
                    log.info("Attempt=" + (context.getRetryCount() + 1) + ": retry running spark job " //
                            + jobClz.getSimpleName());
                    log.warn("Previous failure:", context.getLastThrowable());
                    killLivySession();
                }
                String tenantId = customerSpace.getTenantId();
                String jobName = tenantId + "~" + jobClz.getSimpleName() + "~" + getClass().getSimpleName();
                LivySession session = createLivySession(jobName);
                return sparkJobService.runJob(session, jobClz, jobConfig);
            });
        } finally {
            killLivySession();
        }
    }

    protected String getRandomWorkspace() {
        String workSpace = PathBuilder.buildRandomWorkspacePath(podId, customerSpace).toString();
        workSpaces.add(workSpace);
        return workSpace;
    }

    protected void keepRandomWorkspace(String workSpace) {
        workSpaces.remove(workSpace);
    }

    protected void clearTempData(DataUnit tempData) {
        new Thread(() -> {
            if (tempData instanceof HdfsDataUnit) {
                HdfsDataUnit hdfsDataUnit = (HdfsDataUnit) tempData;
                String avroDir = PathUtils.toParquetOrAvroDir(hdfsDataUnit.getPath());
                try {
                    if (HdfsUtils.isDirectory(yarnConfiguration, avroDir)) {
                        HdfsUtils.rmdir(yarnConfiguration, avroDir);
                    }
                } catch (Exception e) {
                    log.warn("Failed to clear temp data in hdfs: {}", avroDir, e);
                }
            }
        }).start();
    }

    private void clearAllWorkspaces() {
        List<String> toBeRemoved = new ArrayList<>();
        for (String workSpace : workSpaces) {
            try {
                if (HdfsUtils.isDirectory(yarnConfiguration, workSpace)) {
                    HdfsUtils.rmdir(yarnConfiguration, workSpace);
                }
                toBeRemoved.add(workSpace);
            } catch (Exception e) {
                log.warn("Failed to remove workspace {}", workSpace, e);
            }
        }
        workSpaces.retainAll(toBeRemoved);
    }

    // S3 or HDFS
    protected DataUnit toDataUnit(Table table, String alias) {
        String tableName = table.getName();
        HdfsDataUnit hdfsDataUnit = table.toHdfsDataUnit(alias);
        String hdfsDir = PathUtils.toParquetOrAvroDir(hdfsDataUnit.getPath());
        boolean inHdfs = false;
        try {
            inHdfs = HdfsUtils.isDirectory(yarnConfiguration, hdfsDir);
        } catch (IOException e) {
            log.warn("Failed to check if data path exists in hdfs: {}", hdfsDir);
        }
        if (inHdfs) {
            log.info("Found table data in hdfs for {} at {}", tableName, hdfsDir);
            return hdfsDataUnit;
        } else {
            S3DataUnit s3DataUnit = (S3DataUnit) dataUnitProxy.getByNameAndType(customerSpace.toString(), tableName, S3);
            if (s3DataUnit != null) {
                log.info("Found s3 data unit for {} at {}/{}", //
                        tableName, s3DataUnit.getBucket(), s3DataUnit.getPrefix());
                s3DataUnit.setName(alias);
                return s3DataUnit;
            } else {
                throw new IllegalStateException("Cannot find data for " + tableName + " in either HDFS or S3");
            }
        }
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

    protected S3DataUnit toS3DataUnit(HdfsDataUnit jobTarget, BusinessEntity entity, String templateId, List<DataUnit.Role> roles) {
        return SparkUtils.hdfsUnitToS3DataUnit(jobTarget, yarnConfiguration, entity, podId, customerSpace, s3Bucket, templateId, roles);
    }

    protected Table toTable(String tableName, HdfsDataUnit jobTarget) {
        return toTable(tableName, null, jobTarget);
    }

    protected boolean exportToS3(Table table) {
        return exportToS3(table, true);
    }

    protected boolean exportToS3(Table table, boolean sync) {
        return exportToS3(table, sync, null);
    }

    protected boolean exportToS3(Table table, boolean sync, DataUnit.DataFormat format) {
        String tableName = table.getName();
        boolean shouldSkip = Boolean.TRUE.equals(getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class));
        if (!shouldSkip) {
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            String queueName = LedpQueueAssigner.getEaiQueueNameForSubmission();
            queueName = LedpQueueAssigner.overwriteQueueAssignment(queueName, emrEnvService.getYarnQueueScheme());
            ImportExportRequest batchStoreRequest = ImportExportRequest.exportAtlasTable( //
                    customerSpace.toString(), table, //
                    pathBuilder, s3Bucket, podId, //
                    yarnConfiguration, format, //
                    fileStatus -> true);
            if (batchStoreRequest == null) {
                throw new IllegalArgumentException("Cannot construct proper export request for " + tableName);
            }
            HdfsS3ImporterExporter exporter = new HdfsS3ImporterExporter( //
                    customerSpace.toString(), distCpConfiguration, queueName, dataUnitProxy, batchStoreRequest);
            if (sync) {
                exporter.run();
            } else {
                ExecutorService pool = getS3ExporterPool();
                pool.execute(exporter);
            }
        }
        return shouldSkip;
    }

    protected ExecutorService getS3ExporterPool() {
        if (s3ExporterPool == null) {
            s3ExporterPool = Executors.newFixedThreadPool(4);
        }
        return s3ExporterPool;
    }

    protected Map<String, String> exportToS3AndAddToContext(Map<String, Table> tables, String contextKey) {
        Map<String, String> tableNames = tables.entrySet() //
                .stream() //
                .map(entry -> Pair.of(entry.getKey(), entry.getValue().getName())) //
                .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
        for (Map.Entry<String, Table> entry : tables.entrySet()) {
            boolean skipped = exportToS3(entry.getValue(), false);
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
        boolean skipped = exportToS3(table, false);
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
