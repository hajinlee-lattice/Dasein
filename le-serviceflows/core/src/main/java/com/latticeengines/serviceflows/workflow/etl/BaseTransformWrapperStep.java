package com.latticeengines.serviceflows.workflow.etl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.yarn.client.YarnClient;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.workflow.exposed.build.BaseWrapperStep;

public abstract class BaseTransformWrapperStep<T extends BaseWrapperStepConfiguration>
        extends BaseWrapperStep<T, TransformationWorkflowConfiguration> {

    private static final ObjectMapper OM = new ObjectMapper();

    @Autowired
    protected TransformationProxy transformationProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected YarnClient yarnClient;

    @Value("${pls.cdl.transform.cascading.partitions}")
    protected int cascadingPartitions;

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    @Value("${pls.cdl.transform.default.cascading.engine}")
    private String defaultEngine;

    @Value("${pls.cdl.transform.sort.max.split.threads}")
    protected int maxSplitThreads;

    protected String pipelineVersion;

    @Override
    public TransformationWorkflowConfiguration executePreProcessing() {
        return executePreTransformation();
    }

    @Override
    public void onPostProcessingCompleted() {
        pipelineVersion = getStringValueFromContext(TRANSFORM_PIPELINE_VERSION);
        onPostTransformationCompleted();
    }

    protected abstract TransformationWorkflowConfiguration executePreTransformation();
    protected abstract void onPostTransformationCompleted();

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
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions);
        return engineConf;
    }

    protected TransformationFlowParameters.EngineConfiguration heavyEngineConfig2() {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine("TEZ");
        Map<String, String> jobProperties = new HashMap<>();
        jobProperties.put("tez.task.resource.cpu.vcores", String.valueOf(tezVCores * 2));
        jobProperties.put("tez.task.resource.memory.mb", String.valueOf(tezMemGb * 1024 * 2));
        jobProperties.put("mapreduce.job.reduces", String.valueOf(cascadingPartitions / 2));
        engineConf.setJobProperties(jobProperties);
        engineConf.setPartitions(cascadingPartitions);
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
