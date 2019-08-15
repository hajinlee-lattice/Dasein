package com.latticeengines.serviceflows.workflow.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.serviceflows.workflow.util.ETLEngineLoad;
import com.latticeengines.serviceflows.workflow.util.HdfsS3ImporterExporter;
import com.latticeengines.serviceflows.workflow.util.ImportExportRequest;
import com.latticeengines.workflow.exposed.build.BaseWrapperStep;
import com.latticeengines.yarn.exposed.service.EMREnvService;

public abstract class BaseTransformWrapperStep<T extends BaseWrapperStepConfiguration>
        extends BaseWrapperStep<T, TransformationWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BaseTransformWrapperStep.class);

    private static final ObjectMapper OM = new ObjectMapper();

    @Inject
    protected TransformationProxy transformationProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    protected YarnClient yarnClient;

    @Inject
    protected MetadataProxy metadataProxy;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

    @Value("${pls.cdl.transform.spark.executors}")
    private int sparkExecutors;

    @Value("${pls.cdl.transform.tez.am.mem.gb}")
    private int tezAmMemGb; // requested memory for application master

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    @Value("${pls.cdl.transform.default.cascading.engine}")
    private String defaultEngine;

    @Value("${pls.cdl.transform.sort.max.split.threads}")
    protected int maxSplitThreads;

    @Value("${pls.cdl.transform.extra.heavy.multiplier}")
    private int extraHeavyMultiplier;

    @Inject
    private EMREnvService emrEnvService;

    @Inject
    private DataUnitProxy dataUnitProxy;

    @Resource(name = "distCpConfiguration")
    protected Configuration distCpConfiguration;

    @Value("${hadoop.use.emr}")
    private Boolean useEmr;

    @Value("${camille.zk.pod.id}")
    protected String podId;

    @Value("${aws.customer.s3.bucket}")
    protected String s3Bucket;

    protected CustomerSpace customerSpace;
    protected String pipelineVersion;
    protected int scalingMultiplier = 1;

    @Override
    public TransformationWorkflowConfiguration executePreProcessing() {
        return executePreTransformation();
    }

    @Override
    public void onPostProcessingCompleted() {
        String rootId = getRootOperationId(workflowConf);
        if (StringUtils.isNotBlank(rootId)) {
            TransformationProgress progress = transformationProxy.getProgress(rootId);
            if (ProgressStatus.FAILED.equals(progress.getStatus())) {
                throw new RuntimeException(
                        "Transformation failed, check log for detail.: " + JsonUtils.serialize(progress));
            }
        }
        pipelineVersion = getStringValueFromContext(TRANSFORM_PIPELINE_VERSION);
        onPostTransformationCompleted();
    }

    protected abstract TransformationWorkflowConfiguration executePreTransformation();

    protected abstract void onPostTransformationCompleted();

    private String getRootOperationId(TransformationWorkflowConfiguration workflowConf) {
        if (workflowConf != null) {
            try {
                String prepareClz = PrepareTransformationStepInputConfiguration.class.getSimpleName();
                String prepareConfStr = workflowConf.getStepConfigRegistry().getOrDefault(prepareClz, "");
                if (StringUtils.isNotBlank(prepareConfStr)) {
                    PrepareTransformationStepInputConfiguration prepareConf = //
                            JsonUtils.deserialize(prepareConfStr, PrepareTransformationStepInputConfiguration.class);
                    String transformationConfigurationStr = prepareConf.getTransformationConfiguration();
                    PipelineTransformationConfiguration transformationConf = JsonUtils
                            .deserialize(transformationConfigurationStr, PipelineTransformationConfiguration.class);
                    return transformationConf.getRootOperationId();
                }
            } catch (Exception e) {
                log.warn("Failed to extract root operation id from workflow conf.", e);
            }
        }
        return "";
    }

    @Override
    protected Class<TransformationWorkflowConfiguration> getWrappedWorkflowConfClass() {
        return TransformationWorkflowConfiguration.class;
    }

    protected String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion("").getVersion();
    }

    protected String appendEngineConf(TransformerConfig conf,
            TransformationFlowParameters.EngineConfiguration engineConf) {
        ObjectNode on = OM.valueToTree(conf);
        on.set("EngineConfig", OM.valueToTree(engineConf));
        return JsonUtils.serialize(on);
    }

    protected String appendEngineConf(SparkJobConfig jobConfig,
                                      TransformationFlowParameters.EngineConfiguration engineConf) {
        ObjectNode on = OM.valueToTree(jobConfig);
        on.set("EngineConfig", OM.valueToTree(engineConf));
        return JsonUtils.serialize(on);
    }

    protected String emptyStepConfig(TransformationFlowParameters.EngineConfiguration engineConf) {
        return appendEngineConf(new TransformerConfig(), engineConf);
    }

    protected void addBaseTables(TransformationStepConfig step, String... sourceTableNames) {
        if (customerSpace == null) {
            throw new IllegalArgumentException("Have not set customerSpace.");
        }
        List<String> baseSources = step.getBaseSources();
        if (CollectionUtils.isEmpty(baseSources)) {
            baseSources = new ArrayList<>();
        }
        Map<String, SourceTable> baseTables = step.getBaseTables();
        if (MapUtils.isEmpty(baseTables)) {
            baseTables = new HashMap<>();
        }
        for (String sourceTableName: sourceTableNames) {
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            baseSources.add(sourceTableName);
            baseTables.put(sourceTableName, sourceTable);
        }
        step.setBaseSources(baseSources);
        step.setBaseTables(baseTables);
    }

    protected void setTargetTable(TransformationStepConfig step, String tablePrefix) {
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(tablePrefix);
        step.setTargetTable(targetTable);
    }

    protected void setTargetTable(TransformationStepConfig step, String tablePrefix, String primaryKey) {
        TargetTable targetTable = new TargetTable();
        targetTable.setCustomerSpace(customerSpace);
        targetTable.setNamePrefix(tablePrefix);
        targetTable.setPrimaryKey(primaryKey);
        step.setTargetTable(targetTable);
    }

    protected TransformationFlowParameters.EngineConfiguration getEngineConfig(ETLEngineLoad load) {
        switch (load) {
        case LIGHT:
            return lightEngineConfig();
        case HEAVY:
            return heavyEngineConfig();
        case HEAVY_MEMORY:
            return heavyMemoryEngineConfig();
        case EXTRA_HEAVY:
            return extraHeavyEngineConfig();
        default:
            throw new UnsupportedOperationException("Unknown engine load: " + load);
        }
    }

    protected TransformationFlowParameters.EngineConfiguration heavyEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024));
        jobProperties.put("tez.grouping.split-count", String.valueOf(2 * cascadingPartitions * scalingMultiplier));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scalingMultiplier));
        jobProperties.put("spark.dynamicAllocation.maxExecutors", String.valueOf(sparkExecutors * scalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        return engineConf;
    }

    // TODO: Will merge into extraHeavyEngineConfig() in M31
    protected TransformationFlowParameters.EngineConfiguration heavyMemoryEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        int memoryMultiplier = scalingMultiplier == 1 ? 1 : (scalingMultiplier > 3 ? (scalingMultiplier - 1) : 2);
        log.info("Set memoryMultiplier={} based on scalingMultiplier={}", memoryMultiplier, scalingMultiplier);
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * memoryMultiplier));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024 * memoryMultiplier));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024 * memoryMultiplier));
        jobProperties.put("tez.grouping.split-count", String.valueOf(2 * cascadingPartitions * scalingMultiplier));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scalingMultiplier));
        jobProperties.put("spark.dynamicAllocation.maxExecutors", String.valueOf(sparkExecutors * scalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration extraHeavyEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * extraHeavyMultiplier));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024 * extraHeavyMultiplier));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024 * extraHeavyMultiplier));
        jobProperties.put("tez.grouping.split-count", String.valueOf(2 * cascadingPartitions * scalingMultiplier));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scalingMultiplier));
        jobProperties.put("spark.dynamicAllocation.maxExecutors", String.valueOf(sparkExecutors * scalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration lightEngineConfig() {
        if ("FLINK".equalsIgnoreCase(defaultEngine) && scalingMultiplier == 1) {
            TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
            engineConf.setEngine("FLINK");
            engineConf.setPartitions(cascadingPartitions);
            return engineConf;
        } else {
            return heavyEngineConfig();
        }
    }

    protected void exportToS3AndAddToContext(String tableName, String contextKey) {
        exportTableToS3(tableName, contextKey);
        putStringValueInContext(contextKey, tableName);
    }

    protected void exportToS3AndAddToContext(List<String> tableNames, String contextKey) {
        if (CollectionUtils.isEmpty(tableNames)) {
            log.warn("Tries to export empty tableName list for contextKey = {}", contextKey);
            return;
        }

        tableNames.forEach(tableName -> exportTableToS3(tableName, contextKey));
        log.info("Published tables = {}, contextKey = {}", tableNames, contextKey);
        putObjectInContext(contextKey, tableNames);
    }

    protected void exportTableToS3(String tableName, String contextKey) {
        boolean shouldSkip = getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class);
        if (!shouldSkip) {
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            String queueName = LedpQueueAssigner.getEaiQueueNameForSubmission();
            queueName = LedpQueueAssigner.overwriteQueueAssignment(queueName, emrEnvService.getYarnQueueScheme());
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
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
    }
}
