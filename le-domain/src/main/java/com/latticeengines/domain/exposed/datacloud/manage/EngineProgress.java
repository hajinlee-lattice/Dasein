package com.latticeengines.domain.exposed.datacloud.manage;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class EngineProgress {
    private Engine engine;
    private String name;    // Ingestion name/Transformation pipeline name/Publication name
    private String version;
    private ProgressStatus status;
    private Float progress;
    private String message;

    public enum Engine {
        INGESTION, TRANSFORMATION, PUBLICATION
    }

    public EngineProgress() {

    }

    public EngineProgress(Engine engine, String name, String version, ProgressStatus status, Float progress,
            String message) {
        this.engine = engine;
        this.name = name;
        this.version = version;
        this.status = status;
        this.progress = progress;
        this.message = message;
    }

    @JsonProperty("Engine")
    public Engine getEngine() {
        return engine;
    }

    @JsonProperty("Engine")
    public void setEngine(Engine engine) {
        this.engine = engine;
    }

    @JsonProperty("Name")
    public String getName() {
        return name;
    }

    @JsonProperty("Name")
    public void setName(String name) {
        this.name = name;
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
