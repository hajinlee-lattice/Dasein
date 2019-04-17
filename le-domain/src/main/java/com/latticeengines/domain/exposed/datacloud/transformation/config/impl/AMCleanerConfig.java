package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMCleanerConfig extends TransformerConfig {

    @JsonProperty("IsUpdate")
    private boolean isUpdate; // default is false

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    @JsonProperty("IsMini")
    private boolean isMini; // Indicate it's full AM or mini AM

    public boolean getIsUpdate() {
        return isUpdate;
    }

    public void setIsUpdate(boolean isUpdate) {
        this.isUpdate = isUpdate;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public boolean getIsMini() {
        return isMini;
    }

    public void setIsMini(boolean isMini) {
        this.isMini = isMini;
    }
}
