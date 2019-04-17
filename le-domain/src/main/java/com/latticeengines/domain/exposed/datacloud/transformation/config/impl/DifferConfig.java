package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DifferConfig extends TransformerConfig {
    @JsonProperty("Keys")
    private String[] keys;

    @JsonProperty("DiffVersion")
    private String diffVersion; // By default, use latest version

    @JsonProperty("DiffVersionCompared")
    private String diffVersionCompared; // By default, use second to latest
                                        // version

    @JsonProperty("ExcludeFields")
    private String[] excludeFields;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // By default, use latest approved version

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

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

}
