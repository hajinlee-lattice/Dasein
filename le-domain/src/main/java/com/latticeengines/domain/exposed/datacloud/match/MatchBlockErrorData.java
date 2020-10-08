package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MatchBlockErrorData {
    @JsonProperty
    private String containerPath;

    @JsonProperty
    private String errorFilePath;

    @JsonProperty
    private String inputAvro;

    public String getContainerPath() {
        return containerPath;
    }

    public void setContainerPath(String containerPath) {
        this.containerPath = containerPath;
    }

    public String getErrorFilePath() {
        return errorFilePath;
    }

    public void setErrorFilePath(String errorFilePath) {
        this.errorFilePath = errorFilePath;
    }

    public String getInputAvro() {
        return inputAvro;
    }

    public void setInputAvro(String inputAvro) {
        this.inputAvro = inputAvro;
    }
}

