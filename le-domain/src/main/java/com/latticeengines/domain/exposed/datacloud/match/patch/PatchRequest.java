package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Base request entity for DataCloud Patcher
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchRequest {
    @JsonProperty("Mode")
    private PatchMode mode;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    public PatchMode getMode() {
        return mode;
    }

    public void setMode(PatchMode mode) {
        this.mode = mode;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }
}
