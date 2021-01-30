package com.latticeengines.serviceflows.workflow.etl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.apache.avro.Schema;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.yarn.LedpQueueAssigner;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.step.SourceTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TargetTable;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;
import com.latticeengines.domain.exposed.metadata.MigrationTrack;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.util.HdfsToS3PathBuilder;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.DataUnitProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
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
        addBaseTables(step, null, sourceTableNames);
    }

    protected void addBaseTables(TransformationStepConfig step, List<List<String>> partitionKeys,
            String... sourceTableNames) {
        if (partitionKeys != null) {
            Preconditions.checkArgument(partitionKeys.size() == sourceTableNames.length,
                    String.format("Partition key list size %d should be the same as source table name list size %d",
                            partitionKeys.size(), ArrayUtils.getLength(sourceTableNames)));
        }
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
        for (int i = 0; i < sourceTableNames.length; i++) {
            String sourceTableName = sourceTableNames[i];
            SourceTable sourceTable = new SourceTable(sourceTableName, customerSpace);
            if (partitionKeys != null) {
                sourceTable.setPartitionKeys(partitionKeys.get(i));
            }
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
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        engineConf.setScalingMultiplier(scalingMultiplier);
        return engineConf;
    }

    // TODO: Will merge into extraHeavyEngineConfig() in M31
    protected TransformationFlowParameters.EngineConfiguration heavyMemoryEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        // scalingMultiplier is in range [1, 5]
        int scaleUp = scalingMultiplier > 2 ? 3 : Math.max(1, scalingMultiplier);
        int scaleOut = scalingMultiplier > 2 ? scalingMultiplier - 1 : 1;
        log.info("Set scaleUp={} and scaleOut={} based on scalingMultiplier={}", scaleUp, scaleOut, scalingMultiplier);
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * scaleUp));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 2048 * scaleUp));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024 * scaleUp));
        jobProperties.put("tez.grouping.split-count", String.valueOf(2 * cascadingPartitions * scaleOut));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scaleOut));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        engineConf.setScalingMultiplier(scalingMultiplier);
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
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        engineConf.setScalingMultiplier(scalingMultiplier);
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

    protected void exportToS3AndAddToContext(Map<String, String> tableNames, String contextKey) {
        if (MapUtils.isEmpty(tableNames)) {
            log.warn("Tries to export empty tableName map for contextKey = {}", contextKey);
            return;
        }

        tableNames.values().forEach(tableName -> exportTableToS3(tableName, contextKey));
        log.info("Published tables = {}, contextKey = {}", tableNames, contextKey);
        putObjectInContext(contextKey, tableNames);
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

    private void exportTableToS3(String tableName, String contextKey) {
        boolean shouldSkip = getObjectFromContext(SKIP_PUBLISH_PA_TO_S3, Boolean.class);
        if (!shouldSkip) {
            HdfsToS3PathBuilder pathBuilder = new HdfsToS3PathBuilder(useEmr);
            String queueName = LedpQueueAssigner.getEaiQueueNameForSubmission();
            queueName = LedpQueueAssigner.overwriteQueueAssignment(queueName, emrEnvService.getYarnQueueScheme());
            Table table = metadataProxy.getTable(customerSpace.toString(), tableName);
            ImportExportRequest batchStoreRequest = ImportExportRequest.exportAtlasTable( //
                    customerSpace.toString(), table, //
                    pathBuilder, s3Bucket, podId, //
                    yarnConfiguration, null, //
                    fileStatus -> true);
            if (batchStoreRequest == null) {
                throw new IllegalArgumentException("Cannot construct proper export request for " + tableName);
            }
            HdfsS3ImporterExporter exporter = new HdfsS3ImporterExporter( //
                    customerSpace.toString(), distCpConfiguration, queueName, dataUnitProxy, batchStoreRequest);
            new Thread(exporter).start();
        } else {
            log.info("Skip publish " + contextKey + " (" + tableName + ") to S3.");
        }
    }

    protected void writeSchema(Table table) {
        try {
            Schema schema = TableUtils.createSchema(AvroUtils.getAvroFriendlyString(table.getName()), table);
            String avscPath = getAvscPath(table);
            log.info("Write schema for table={}, avscPath={}", table.getName(), avscPath);
            HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
        } catch (Exception ex) {
            log.warn("Could not write schema, error=" + ex.getMessage(), ex);
        }
    }

    private String getAvscPath(Table table) {
        String avscFile = table.getName() + ".avsc";
        return PathBuilder.buildDataTableSchemaPath(podId, customerSpace).append(table.getName()).append(avscFile)
                .toString();
    }

    protected boolean inMigrationMode() {
        MigrationTrack.Status status = metadataProxy.getMigrationStatus(customerSpace.toString());
        log.info("Tenant's migration status is {}.", status);
        boolean migrationMode = MigrationTrack.Status.STARTED.equals(status);
        log.info("Migration mode is {}", migrationMode ? "on" : "off");
        return migrationMode;
    }

}
