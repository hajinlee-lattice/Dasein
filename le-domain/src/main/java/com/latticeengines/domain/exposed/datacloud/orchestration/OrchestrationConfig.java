package com.latticeengines.domain.exposed.datacloud.orchestration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "ClassName")
@JsonSubTypes({ @JsonSubTypes.Type(value = PredefinedScheduleConfig.class, name = "PredefinedScheduleConfig"),
        @JsonSubTypes.Type(value = EngineTriggeredConfig.class, name = "EngineTriggeredConfig") })
public abstract class OrchestrationConfig {
    private String className;
    private String enginePipelineConfig; // "[{"Engine":"XXX","Name":"XXX"},{"Engine":"XXX","Name":"XXX"}]"
    private List<Pair<DataCloudEngine, String>> enginePipeline;

    private static final String ENGINE = "Engine";
    private static final String NAME = "Name";

    public OrchestrationConfig() {
        setClassName(getClass().getSimpleName());
    }

    @JsonIgnore
    public Pair<DataCloudEngine, String> firstStep() {
        if (enginePipeline == null) {
            initEnginePipeline();
        }
        return enginePipeline.get(0);
    }

    @JsonIgnore
    public Pair<DataCloudEngine, String> nextStep(Pair<DataCloudEngine, String> cur) {
        if (enginePipeline == null) {
            initEnginePipeline();
        }
        Iterator<Pair<DataCloudEngine, String>> iter = enginePipeline.iterator();
        while (iter.hasNext()) {
            Pair<DataCloudEngine, String> node = iter.next();
            if (node.equals(cur)) {
                return iter.hasNext() ? iter.next() : null;
            }
        }
        return null;
    }

    @JsonIgnore
    public List<Pair<DataCloudEngine, String>> getEnginePipeline() {
        if (enginePipeline == null) {
            initEnginePipeline();
        }
        return enginePipeline;
    }

    @JsonIgnore
    private void initEnginePipeline() {
        try {
            enginePipeline = new ArrayList<>();
            ObjectMapper om = new ObjectMapper();
            JsonNode engines = om.readTree(enginePipelineConfig);
            for (JsonNode engine : engines) {
                enginePipeline
                        .add(Pair.of(DataCloudEngine.valueOf(engine.get(ENGINE).asText()), engine.get(NAME).asText()));
            }
        } catch (IOException e) {
            throw new RuntimeException(String.format("Fail to parse enginePipelineConfig: %s", enginePipelineConfig),
                    e);
        }
    }

    @JsonProperty("ClassName")
    private String getClassName() {
        return className;
    }

    @JsonProperty("ClassName")
    private void setClassName(String className) {
        this.className = className;
    }

    @JsonProperty("EnginePipelineConfig")
    private String getEnginePipelineConfig() {
        return enginePipelineConfig;
    }

    @JsonProperty("EnginePipelineConfig")
    private void setEnginePipelineConfig(String enginePipelineConfig) {
        this.enginePipelineConfig = enginePipelineConfig;
    }


}
