package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ConfigurationRequest {

    @JsonProperty("Configuration")
    private EntityMatchConfiguration configuration;

    @JsonProperty("Tenant")
    private Tenant tenant;

    public EntityMatchConfiguration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(EntityMatchConfiguration configuration) {
        this.configuration = configuration;
    }

    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }
}
