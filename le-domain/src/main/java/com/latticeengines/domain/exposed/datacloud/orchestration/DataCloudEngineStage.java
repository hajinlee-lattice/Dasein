package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCloudEngineStage {
    @JsonProperty("Engine")
    private DataCloudEngine engine;
    @JsonProperty("EngineName")
    private String engineName;
    @JsonProperty("Timeout")
    private long timeout; // in minute
    @JsonProperty("Version")
    private String version;
    @JsonProperty("Status")
    private ProgressStatus status;
    @JsonProperty("Progress")
    private Float progress;
    @JsonProperty("Message")
    private String message;

    public DataCloudEngineStage() {

    }

    public DataCloudEngineStage(DataCloudEngine engine, String engineName, long timeout) {
        this.engine = engine;
        this.engineName = engineName;
        this.timeout = timeout;
    }

    public DataCloudEngineStage(DataCloudEngine engine, String engineName, String version) {
        this.engine = engine;
        this.engineName = engineName;
        this.version = version;
    }

    public DataCloudEngine getEngine() {
        return engine;
    }

    public void setEngine(DataCloudEngine engine) {
        this.engine = engine;
    }

    public String getEngineName() {
        return engineName;
    }

    public void setEngineName(String engineName) {
        this.engineName = engineName;
    }

    public long getTimeout() {
        return timeout;
    }

    public void setTimeout(long timeout) {
        this.timeout = timeout;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public ProgressStatus getStatus() {
        return status;
    }

    public void setStatus(ProgressStatus status) {
        this.status = status;
    }

    public Float getProgress() {
        return progress;
    }

    public void setProgress(Float progress) {
        this.progress = progress;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof DataCloudEngineStage)) {
            return false;
        }
        DataCloudEngineStage step = (DataCloudEngineStage) obj;
        return this.engine == step.engine && this.engineName.equals(step.engineName);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
