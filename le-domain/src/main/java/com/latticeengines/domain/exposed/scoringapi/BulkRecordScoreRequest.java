package com.latticeengines.domain.exposed.scoringapi;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

import io.swagger.annotations.ApiModelProperty;

public class BulkRecordScoreRequest {

    @JsonProperty("source")
    @ApiModelProperty(value = "Name of the source system that originated this score request.")
    private String source;

    @JsonProperty("records")
    @ApiModelProperty(value = "List of records", required = true)
    private List<Record> records;

    @ApiModelProperty(hidden = true)
    private String rootOperationId;

    @ApiModelProperty(hidden = true)
    private String requestTimestamp;

    @ApiModelProperty(hidden = true)
    private boolean homogeneous;

    @ApiModelProperty(hidden = true)
    private String dataCloudVersion;

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public List<Record> getRecords() {
        return records;
    }

    public void setRecords(List<Record> records) {
        this.records = records;
    }

    public String getRootOperationId() {
        return rootOperationId;
    }

    public void setRootOperationId(String rootOperationId) {
        this.rootOperationId = rootOperationId;
    }

    public String getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(String requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }

    public boolean isHomogeneous() {
        return homogeneous;
    }

    public void setHomogeneous(boolean homogeneous) {
        this.homogeneous = homogeneous;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }

}
