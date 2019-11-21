package com.latticeengines.domain.exposed.datacloud.orchestration;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Orchestration
 * up to date if there is any new change
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataCloudEngineStage {
    // Type of engine job
    @JsonProperty("Engine")
    private DataCloudEngine engine;

    // Engine job name: Existed ingestion name / transformation pipeline name /
    // publication name
    @JsonProperty("EngineName")
    private String engineName;

    // If single job step in the pipeline runs longer than defined timeout (in
    // minutes), treat the step as failed
    @JsonProperty("Timeout")
    private long timeout; // in minute

    // Version of the engine job pipeline
    // -- Different OrchestrationConfig has different strategy to decide version
    // for an engine job pipeline
    // -- If the pipeline step is an transformation job, the version is set for
    // transformation pipeline version
    // -- If the pipeline step is a publication job, the version is set for
    // "SourceVersion" in publication API body
    // -- Timestamp-format defined in {@link
    // #HdfsPathBuilder.DATA_FORMAT_STRING}
    @JsonProperty("Version")
    private String version;

    // Status of current pipeline step; Not used for configuration; Only for
    // internal tracking
    @JsonProperty("Status")
    private ProgressStatus status;

    // Progress percentage of current pipeline step; Not used for configuration;
    // Only for internal tracking
    @JsonProperty("Progress")
    private Float progress;

    // Tracking message of current pipeline step if it encounters any issue; Not
    // used for configuration; Only for internal tracking
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
