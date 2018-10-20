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

    public SimpleDataFeed() {
    }

    public SimpleDataFeed(Tenant tenant, Status status, Date nextInvokeTime) {
        this.tenant = tenant;
        this.status = status;
        this.nextInvokeTime = nextInvokeTime;
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
}
