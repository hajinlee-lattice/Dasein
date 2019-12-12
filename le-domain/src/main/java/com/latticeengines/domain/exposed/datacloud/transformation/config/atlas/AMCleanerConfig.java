package com.latticeengines.domain.exposed.datacloud.transformation.config.atlas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class AMCleanerConfig extends TransformerConfig {

    @JsonProperty("IsUpdate")
    private boolean isUpdate; // default is false

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

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
}
