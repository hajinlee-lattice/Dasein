package com.latticeengines.domain.exposed.security;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class Session {

    private Tenant tenant;
    private List<String> rights;
    private Ticket ticket;
    
    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }
    
    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @JsonProperty("Rights")
    public List<String> getRights() {
        return rights;
    }

    @JsonProperty("Rights")
    public void setRights(List<String> rights) {
        this.rights = rights;
    }

    @JsonProperty("Ticket")
    public Ticket getTicket() {
        return ticket;
    }

    @JsonProperty("Ticket")
    public void setTicket(Ticket ticket) {
        this.ticket = ticket;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
