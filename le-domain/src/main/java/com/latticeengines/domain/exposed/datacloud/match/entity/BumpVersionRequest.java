package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Arrays;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

/*
 * Request object to bump up versions of entity match for specific tenant
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BumpVersionRequest {

    @JsonProperty("Tenant")
    private Tenant tenant;

    @JsonProperty("Environments")
    private List<EntityMatchEnvironment> environments;

    public BumpVersionRequest() {
    }

    public BumpVersionRequest(Tenant tenant, EntityMatchEnvironment... environments) {
        this.tenant = tenant;
        if (environments != null) {
            this.environments = Arrays.asList(environments);
        }
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public List<EntityMatchEnvironment> getEnvironments() {
        return environments;
    }

    public void setEnvironments(List<EntityMatchEnvironment> environments) {
        this.environments = environments;
    }
}
