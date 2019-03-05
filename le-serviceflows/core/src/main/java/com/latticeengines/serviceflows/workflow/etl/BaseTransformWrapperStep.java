package com.latticeengines.serviceflows.workflow.etl;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.PipelineTransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.PrepareTransformationStepInputConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWrapperStep;

public abstract class BaseTransformWrapperStep<T extends BaseWrapperStepConfiguration>
        extends BaseWrapperStep<T, TransformationWorkflowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BaseTransformWrapperStep.class);

    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    protected TransformationProxy transformationProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected YarnClient yarnClient;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

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

    protected String pipelineVersion;

    protected int scalingMultiplier = 1;
    protected int extraScalingMultiplier = 2;

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

    protected String emptyStepConfig(TransformationFlowParameters.EngineConfiguration engineConf) {
        return appendEngineConf(new TransformerConfig(), engineConf);
    }

    protected TransformationFlowParameters.EngineConfiguration heavyEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * scalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * scalingMultiplier);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration extraHeavyEngineConfig() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * extraScalingMultiplier));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024 * extraScalingMultiplier));
        jobProperties.put("tez.am.resource.memory.mb", String.valueOf(tezAmMemGb * 1024 * extraScalingMultiplier));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions * extraScalingMultiplier));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions * extraScalingMultiplier);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration lightEngineConfig() {
        if ("FLINK".equalsIgnoreCase(defaultEngine)) {
            TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
            engineConf.setEngine("FLINK");
            engineConf.setPartitions(cascadingPartitions);
            return engineConf;
        } else {
            return heavyEngineConfig();
        }
    }
}
