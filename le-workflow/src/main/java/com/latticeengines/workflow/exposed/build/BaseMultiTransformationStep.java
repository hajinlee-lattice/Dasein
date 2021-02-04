package com.latticeengines.workflow.exposed.build;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.PathUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;
import com.latticeengines.domain.exposed.workflow.BaseMultiTransformationStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;

public abstract class BaseMultiTransformationStep<T extends BaseMultiTransformationStepConfiguration> extends BaseWorkflowStep<T> {

    private static final Logger log = LoggerFactory.getLogger(BaseMultiTransformationStep.class);

    private static final ObjectMapper OM = new ObjectMapper();

    @Value("${pls.cdl.transform.default.cascading.engine}")
    private String defaultEngine;

    @Value("${pls.cdl.transform.cascading.partitions}")
    private int cascadingPartitions;

    @Value("${pls.cdl.transform.spark.executors}")
    private int sparkExecutors;

    @Value("${pls.cdl.transform.tez.am.mem.gb}")
    private int tezAmMemGb; // requested memory for application master

    @Value("${pls.cdl.transform.tez.task.vcores}")
    private int tezVCores;

    @Value("${pls.cdl.transform.tez.task.mem.gb}")
    private int tezMemGb;

    @Inject
    private TransformationProxy transformationProxy;

    private int scalingMultiplier = 1;

    @Override
    public void execute() {
        intializeConfiguration();
        int index = 0;
        TransformationProgress progress = null;
        PipelineTransformationRequest request = null;
        do {
            request = generateRequest(progress, index);
            progress = transformationProxy.transform(request, configuration.getPodId());
            log.info("progress: {}", progress);
            waitForProgressStatus(progress.getRootOperationUID());
        } while (shouldContinue(progress, request, index++));
        onPostTransformationCompleted();
    }

    private ProgressStatus waitForProgressStatus(String rootId) {
        while (true) {
            TransformationProgress progress = transformationProxy.getProgress(rootId);
            log.info("progress is {}", JsonUtils.serialize(progress));
            if (progress == null) {
                throw new IllegalStateException(String.format("transformationProgress cannot be null, rootId is %s.",
                        rootId));
            }
            if (progress.getStatus().equals(ProgressStatus.FINISHED)) {
                return progress.getStatus();
            }
            if (progress.getStatus().equals(ProgressStatus.FAILED)) {
                throw new RuntimeException(
                        "Transformation failed, check log for detail.: " + JsonUtils.serialize(progress));
            }
            try {
                Thread.sleep(120000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected Long getTableDataLines(Table table) {
        if (table == null || table.getExtracts() == null) {
            return 0L;
        }
        Long lines = 0L;
        List<String> paths = new ArrayList<>();
        for (Extract extract : table.getExtracts()) {
            paths.add(PathUtils.toAvroGlob(extract.getPath()));
        }
        for (String path : paths) {
            lines += AvroUtils.count(yarnConfiguration, path);
        }
        return lines;
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

    private TransformationFlowParameters.EngineConfiguration heavyEngineConfig() {
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
        engineConf.setScalingMultiplier(scalingMultiplier);
        return engineConf;
    }

    protected abstract void intializeConfiguration();

    /**
     *
     * @param lastTransformationProgress using this to get some related info which we used to generate a request
     * @param currentIndex current loop index
     * @return return next Transformation Progress
     */
    protected abstract PipelineTransformationRequest generateRequest(TransformationProgress lastTransformationProgress,
                                                                     int currentIndex);

    /**
     *
     * @param lastTransformationProgress using this to get some related info which we used to validate
     * @param lastTransformationRequest using this to get some step info and target Table info
     * @param currentIndex current loop index
     * @return if we can do next loop according to validation
     */
    protected abstract boolean shouldContinue(TransformationProgress lastTransformationProgress,
                                              PipelineTransformationRequest lastTransformationRequest,
                                              int currentIndex);

    protected abstract void onPostTransformationCompleted();
}
