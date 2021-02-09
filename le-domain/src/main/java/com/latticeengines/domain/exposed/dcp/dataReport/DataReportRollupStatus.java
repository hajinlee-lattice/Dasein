package com.latticeengines.domain.exposed.dcp.dataReport;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;

/**
 * For updating the completed status of a rollup.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataReportRollupStatus {

    @JsonProperty("status")
    @Enumerated(EnumType.STRING)
    private DataReportRecord.RollupStatus status;

    @JsonProperty("statusMessage")
    private String statusMessage;

    @JsonProperty("statusCode")
    private String statusCode;

    public DataReportRollupStatus () { }

    public DataReportRollupStatus(DataReportRecord.RollupStatus status) {
        this.status = status;
    }

    public DataReportRollupStatus(DataReportRecord.RollupStatus status, String statusCode, String message) {
        this(status);
        this.statusMessage = message;
        this.statusCode = statusCode;
    }

    public DataReportRecord.RollupStatus getStatus() {
        return status;
    }

    public void setStatus(DataReportRecord.RollupStatus status) {
        this.status = status;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(String statusCode) {
        this.statusCode = statusCode;
    }
}
