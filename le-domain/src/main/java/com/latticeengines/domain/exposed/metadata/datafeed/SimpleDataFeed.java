package com.latticeengines.domain.exposed.metadata.datafeed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.security.Tenant;

public class SimpleDataFeed {

    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonProperty("status")
    private Status status;

    public SimpleDataFeed() {
    }

    public SimpleDataFeed(Tenant tenant, Status status) {
        this.tenant = tenant;
        this.status = status;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public DataFeed.Status getStatus() {
        return status;
    }

    public void setStatus(DataFeed.Status status) {
        this.status = status;
    }
}
