package com.latticeengines.domain.exposed.workflow;

import java.util.Date;
import java.util.EnumSet;

import org.springframework.batch.core.BatchStatus;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class WorkflowStatus {

    public static final EnumSet<BatchStatus> TERMINAL_BATCH_STATUS = EnumSet.of(BatchStatus.ABANDONED,
    BatchStatus.COMPLETED, BatchStatus.FAILED, BatchStatus.STOPPED);

    private BatchStatus status;
    private Date startTime;
    private Date endTime;
    private Date lastUpdated;
    private String workflowName;
    private CustomerSpace customerSpace;

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

    @JsonProperty("workflowName")
    public String getWorkflowName() {
        return workflowName;
    }

    @JsonProperty("workflowName")
    public void setWorkflowName(String workflowName) {
        this.workflowName = workflowName;
    }

    @JsonProperty("customerSpace")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

}
