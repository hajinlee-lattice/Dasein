package com.latticeengines.domain.exposed.modeling.factory;

import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.domain.exposed.modeling.Algorithm;
import com.latticeengines.domain.exposed.modelquality.Pipeline;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyDef;
import com.latticeengines.domain.exposed.modelquality.PipelinePropertyValue;
import com.latticeengines.domain.exposed.modelquality.PipelineStep;
import com.latticeengines.domain.exposed.modelquality.SelectedConfig;

public class PipelineFactory extends ModelFactory {

    private static final Logger log = LoggerFactory.getLogger(PipelineFactory.class);

    public static final String PIPELINE_NAME_KEY = "pipeline.name";

    public static void configPipeline(Algorithm algo, Map<String, String> runTimeParams) {

        log.info("Check and Config Pipeline.");

        Pipeline pipeline = getModelPipeline(runTimeParams);
        if (pipeline == null) {
            return;
        }
        configPipeline(algo, pipeline);
        log.info("Successfully configured the Pipeline");
    }

    private static void configPipeline(Algorithm algo, Pipeline pipeline) {
       
        if (StringUtils.isNotEmpty(pipeline.getPipelineScript())) {
            algo.setPipelineScript(pipeline.getPipelineScript());
        }
        if (StringUtils.isNotEmpty(pipeline.getPipelineLibScript())) {
            algo.setPipelineLibScript(pipeline.getPipelineLibScript());
        }
        if (StringUtils.isNotEmpty(pipeline.getPipelineDriver())) {
            algo.setPipelineDriver(pipeline.getPipelineDriver());
        }
        String pipelineProperties = getPipelineProperties(pipeline);
        if (StringUtils.isNotEmpty(pipelineProperties)) {
            algo.setPipelineProperties(pipelineProperties);
        }
    }

    private static String getPipelineProperties(Pipeline pipeline) {
        if (pipeline == null || CollectionUtils.isEmpty(pipeline.getPipelineSteps())) {
            return null;
        }
        StringBuilder builder = new StringBuilder();
        List<PipelineStep> steps = pipeline.getPipelineSteps();
        for (PipelineStep step : steps) {
            List<PipelinePropertyDef> defs = step.getPipelinePropertyDefs();
            if (CollectionUtils.isEmpty(defs)) {
                continue;
            }
            for (PipelinePropertyDef def : defs) {
                List<PipelinePropertyValue> values = def.getPipelinePropertyValues();
                if (CollectionUtils.isEmpty(values)) {
                    continue;
                }
                for (PipelinePropertyValue value : values) {
                    if (StringUtils.isNotEmpty(value.getValue())) {
                        builder.append(step.getName()).append(".").append(def.getName()).append("=")
                                .append(value.getValue()).append(" ");
                        break;
                    }
                }
            }
        }
        if (builder.length() > 0) {
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }

    private static Pipeline getModelPipeline(Map<String, String> runTimeParams) {
        SelectedConfig selectedConfig = getModelConfig(runTimeParams);
        if (selectedConfig != null) {
            return selectedConfig.getPipeline();
        }
        return null;

    }

}
