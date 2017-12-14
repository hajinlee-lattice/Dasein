package com.latticeengines.domain.exposed.cdl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CleanupByDateRangeConfiguration extends CleanupOperationConfiguration {

    @JsonProperty("start_time")
    private Date startTime;

    @JsonProperty("end_time")
    private Date endTime;

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }
}
