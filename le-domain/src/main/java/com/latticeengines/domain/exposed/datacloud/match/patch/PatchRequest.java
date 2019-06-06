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

    @JsonProperty("Offset")
    private int offset;

    @JsonProperty("Limit")
    private int limit;

    @JsonProperty("sortByField")
    private String sortByField;

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

    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getLimit() {
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    public String getSortByField() {
        return sortByField;
    }

    public void setSortByfield(String sortByField) {
        this.sortByField = sortByField;
    }
}
