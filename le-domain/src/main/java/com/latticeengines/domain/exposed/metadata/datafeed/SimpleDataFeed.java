package com.latticeengines.domain.exposed.metadata.datafeed;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

public class SimpleDataFeed  implements Serializable {

    @JsonProperty("tenant")
    private Tenant tenant;

    @JsonProperty("status")
    private DataFeed.Status status;

    public void setTenant(Tenant tenant) { this.tenant = tenant; }

    public Tenant getTenant() {
        return tenant;
    }

    public DataFeed.Status getStatus() { return status; }

    public void setStatus(DataFeed.Status status) { this.status = status; }
}
