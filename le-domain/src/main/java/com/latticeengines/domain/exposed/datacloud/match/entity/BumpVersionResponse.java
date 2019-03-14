package com.latticeengines.domain.exposed.datacloud.match.entity;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

/*
 * Response object to show the version of entity match after being bumped up
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class BumpVersionResponse {

    @JsonProperty("Tenant")
    private Tenant tenant;

    @JsonProperty("Versions")
    private Map<EntityMatchEnvironment, Integer> versions;

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    public Map<EntityMatchEnvironment, Integer> getVersions() {
        return versions;
    }

    public void setVersions(Map<EntityMatchEnvironment, Integer> versions) {
        this.versions = versions;
    }
}
