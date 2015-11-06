package com.latticeengines.domain.exposed.workflow;

import java.util.Date;

import org.springframework.batch.core.BatchStatus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class WorkflowStatus {

    private BatchStatus status;
    private Date startTime;
    private Date endTime;
    private Date lastUpdated;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("status")
    public BatchStatus getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(BatchStatus status) {
        this.status = status;
    }

    @JsonProperty("startTime")
    public Date getStartTime() {
        return startTime;
    }

    @JsonProperty("startTime")
    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    @JsonProperty("endTime")
    public Date getEndTime() {
        return endTime;
    }

    @JsonProperty("endTime")
    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    @JsonProperty("lastUpdated")
    public Date getLastUpdated() {
        return lastUpdated;
    }

    @JsonProperty("lastUpdated")
    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

}
