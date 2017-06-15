package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrchestrationPipelineStep {
    private DataCloudEngine engine;

    private String engineName;

    private long timeout;

    public OrchestrationPipelineStep() {

    }

    public OrchestrationPipelineStep(DataCloudEngine engine, String engineName, long timeout) {
        this.engine = engine;
        this.engineName = engineName;
        this.timeout = timeout;
    }

    @JsonProperty("Engine")
    public DataCloudEngine getEngine() {
        return engine;
    }

    @JsonProperty("Engine")
    public void setEngine(DataCloudEngine engine) {
        this.engine = engine;
    }

    @JsonProperty("EngineName")
    public String getEngineName() {
        return engineName;
    }

    @JsonProperty("EngineName")
    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    @JsonProperty("Timeout")
    public long getTimeout() {
        return timeout;
    }

    @JsonProperty("Timeout")
    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof OrchestrationPipelineStep)) {
            return false;
        }
        OrchestrationPipelineStep step = (OrchestrationPipelineStep) obj;
        return this.engine == step.engine && this.engineName.equals(step.engineName);
    }

}
