package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public class EngineProgress {
    private DataCloudEngine engine;
    private String engineName;    // Ingestion name/Transformation pipeline name/Publication name
    private String version;
    private ProgressStatus status;
    private Float progress;
    private String message;

    public EngineProgress() {

    }

    public EngineProgress(DataCloudEngine engine, String engineName, String version, ProgressStatus status,
            Float progress,
            String message) {
        this.engine = engine;
        this.engineName = engineName;
        this.version = version;
        this.status = status;
        this.progress = progress;
        this.message = message;
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

    @JsonProperty("Version")
    public String getVersion() {
        return version;
    }

    @JsonProperty("Version")
    public void setVersion(String version) {
        this.version = version;
    }

    @JsonProperty("Status")
    public ProgressStatus getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(ProgressStatus status) {
        this.status = status;
    }

    @JsonProperty("Progress")
    public Float getProgress() {
        return progress;
    }

    @JsonProperty("Progress")
    public void setProgress(Float progress) {
        this.progress = progress;
    }

    @JsonProperty("Message")
    public String getMessage() {
        return message;
    }

    @JsonProperty("Message")
    public void setMessage(String message) {
        this.message = message;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
