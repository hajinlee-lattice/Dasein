package com.latticeengines.domain.exposed.datacloud.orchestration;

import java.util.Iterator;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = PredefinedScheduleConfig.class, name = "PredefinedScheduleConfig"),
        @JsonSubTypes.Type(value = ExternalTriggerConfig.class, name = "ExternalTriggerConfig") })
public abstract class OrchestrationConfig {
    private String className;
    private String pipelineConfig;
    private List<OrchestrationPipelineStep> pipeline;

    public OrchestrationConfig() {
        setClassName(getClass().getSimpleName());
    }

    @JsonIgnore
    public OrchestrationPipelineStep firstStep() {
        if (pipeline == null) {
            initEnginePipeline();
        }
        return pipeline.get(0);
    }

    @JsonIgnore
    public OrchestrationPipelineStep nextStep(OrchestrationPipelineStep cur) {
        if (pipeline == null) {
            initEnginePipeline();
        }
        Iterator<OrchestrationPipelineStep> iter = pipeline.iterator();
        while (iter.hasNext()) {
            OrchestrationPipelineStep step = iter.next();
            if (step.equals(cur)) {
                return iter.hasNext() ? iter.next() : null;
            }
        }
        return null;
    }

    @JsonIgnore
    public List<OrchestrationPipelineStep> getEnginePipeline() {
        if (pipeline == null) {
            initEnginePipeline();
        }
        return pipeline;
    }

    @SuppressWarnings("rawtypes")
    @JsonIgnore
    private void initEnginePipeline() {
        List list = JsonUtils.deserialize(pipelineConfig, List.class);
        pipeline = JsonUtils.convertList(list, OrchestrationPipelineStep.class);
    }

    @JsonProperty("ClassName")
    private String getClassName() {
        return className;
    }

    @JsonProperty("ClassName")
    private void setClassName(String className) {
        this.className = className;
    }

    @JsonProperty("PipelineConfig")
    public String getPipelineConfig() {
        return pipelineConfig;
    }

    @JsonProperty("PipelineConfig")
    private void setPipelineConfig(String pipelineConfig) {
        this.pipelineConfig = pipelineConfig;
    }


}
