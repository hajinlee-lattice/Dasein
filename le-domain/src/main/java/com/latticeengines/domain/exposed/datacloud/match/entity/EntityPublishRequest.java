package com.latticeengines.domain.exposed.datacloud.match.entity;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class EntityPublishRequest {
    @JsonProperty("Entity")
    private String entity;

    @JsonProperty("SrcTenant")
    private Tenant srcTenant;

    @JsonProperty("DestTenant")
    private Tenant destTenant;

    @JsonProperty("DestEnv")
    private EntityMatchEnvironment destEnv;

    @JsonProperty("DestTTLEnabled")
    private Boolean destTTLEnabled;

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public Tenant getSrcTenant() {
        return srcTenant;
    }

    public void setSrcTenant(Tenant srcTenant) {
        this.srcTenant = srcTenant;
    }

    public Tenant getDestTenant() {
        return destTenant;
    }

    public void setDestTenant(Tenant destTenant) {
        this.destTenant = destTenant;
    }

    public EntityMatchEnvironment getDestEnv() {
        return destEnv;
    }

    public void setDestEnv(EntityMatchEnvironment destEnv) {
        this.destEnv = destEnv;
    }

    public Boolean getDestTTLEnabled() {
        return destTTLEnabled;
    }

    public void setDestTTLEnabled(Boolean destTTLEnabled) {
        this.destTTLEnabled = destTTLEnabled;
    }
}
