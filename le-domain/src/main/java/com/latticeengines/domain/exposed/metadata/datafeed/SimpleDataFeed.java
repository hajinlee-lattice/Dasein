package com.latticeengines.domain.exposed.metadata.datafeed;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.security.Tenant;

public class SimpleDataFeed {

    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonProperty("status")
    private Status status;

    @JsonProperty("nextinvoketime")
    private Date nextInvokeTime;

    @JsonProperty("schedulenow")
    private boolean scheduleNow = false;

    @JsonProperty("scheduletime")
    private Date scheduleTime;

    @JsonProperty("schedulerequest")
    private String scheduleRequest;

    public SimpleDataFeed() {
    }

    public SimpleDataFeed(Tenant tenant, Status status, Date nextInvokeTime, Boolean scheduleNow, Date scheduleTime, String scheduleRequest) {
        this.tenant = tenant;
        this.status = status;
        this.nextInvokeTime = nextInvokeTime;
        this.scheduleNow = scheduleNow;
        this.scheduleTime = scheduleTime;
        this.scheduleRequest = scheduleRequest;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public DataFeed.Status getStatus() {
        return status;
    }

    public void setStatus(DataFeed.Status status) {
        this.status = status;
    }

    public Date getNextInvokeTime() {
        return nextInvokeTime;
    }

    public void setNextInvokeTime(Date nextInvokeTime) {
        this.nextInvokeTime = nextInvokeTime;
    }

    public boolean isScheduleNow() {
        return scheduleNow;
    }

    public void setScheduleNow(boolean scheduleNow) {
        this.scheduleNow = scheduleNow;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public String getScheduleRequest() {
        return scheduleRequest;
    }

    public void setScheduleRequest(String scheduleRequest) {
        this.scheduleRequest = scheduleRequest;
    }
}
