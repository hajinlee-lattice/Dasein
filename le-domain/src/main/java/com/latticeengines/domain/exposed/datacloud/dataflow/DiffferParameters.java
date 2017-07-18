package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DiffferParameters extends TransformationFlowParameters {
    @JsonProperty("Keys")
    private String[] keys;

    @JsonProperty("DiffVersion")
    private String diffVersion;

    @JsonProperty("DiffVersionCompared")
    private String diffVersionCompared;

    @JsonProperty("ExcludeFields")
    private String[] excludeFields;

    public String[] getKeys() {
        return keys;
    }

    public void setKeys(String[] keys) {
        this.keys = keys;
    }

    public String getDiffVersion() {
        return diffVersion;
    }

    public void setDiffVersion(String diffVersion) {
        this.diffVersion = diffVersion;
    }

    public String getDiffVersionCompared() {
        return diffVersionCompared;
    }

    public void setDiffVersionCompared(String diffVersionCompared) {
        this.diffVersionCompared = diffVersionCompared;
    }

    public String[] getExcludeFields() {
        return excludeFields;
    }

    public void setExcludeFields(String[] excludeFields) {
        this.excludeFields = excludeFields;
    }
}
