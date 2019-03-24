package com.latticeengines.domain.exposed.datacloud.orchestration;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({
        @JsonSubTypes.Type(value = PredefinedScheduleConfig.class, name = "PredefinedScheduleConfig"),
        @JsonSubTypes.Type(value = ExternalTriggerConfig.class, name = "ExternalTriggerConfig") })
public abstract class OrchestrationConfig {
    @JsonProperty("ClassName")
    private String className;

    @JsonProperty("PipelineConfig")
    private List<DataCloudEngineStage> pipeline;

    public OrchestrationConfig() {
        setClassName(getClass().getSimpleName());
    }

    @JsonIgnore
    public DataCloudEngineStage firstStage() {
        if (CollectionUtils.isEmpty(pipeline)) {
            throw new RuntimeException("PipelinConfig is empty");
        }
        return pipeline.get(0);
    }

    @JsonIgnore
    public DataCloudEngineStage nextStage(DataCloudEngineStage cur) {
        if (CollectionUtils.isEmpty(pipeline)) {
            throw new RuntimeException("PipelinConfig is empty");
        }
        Iterator<DataCloudEngineStage> iter = pipeline.iterator();
        while (iter.hasNext()) {
            DataCloudEngineStage step = iter.next();
            if (step.equals(cur)) {
                return iter.hasNext() ? iter.next() : null;
            }
        }
        return null;
    }

    private void setClassName(String className) {
        this.className = className;
    }

    public List<DataCloudEngineStage> getPipeline() {
        return pipeline;
    }

    public void setPipeline(List<DataCloudEngineStage> pipeline) {
        this.pipeline = pipeline;
    }

}
