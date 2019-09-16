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

    @JsonProperty("SrcVersion")
    private Integer srcVersion;

    @JsonProperty("DestTenant")
    private Tenant destTenant;

    @JsonProperty("DestVersion")
    private Integer destVersion;

    @JsonProperty("DestEnv")
    private EntityMatchEnvironment destEnv;

    // If not set, will use default TTL setting of DestEnv
    @JsonProperty("DestTTLEnabled")
    private Boolean destTTLEnabled;

    // Entity publish API is for testing purpose. For most of cases, we
    // might want dest env to start with empty universe
    @JsonProperty("BumpupVersion")
    private boolean bumpupVersion = true;

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

    public Integer getSrcVersion() {
        return srcVersion;
    }

    public void setSrcVersion(Integer srcVersion) {
        this.srcVersion = srcVersion;
    }

    public Tenant getDestTenant() {
        return destTenant;
    }

    public void setDestTenant(Tenant destTenant) {
        this.destTenant = destTenant;
    }

    public Integer getDestVersion() {
        return destVersion;
    }

    public void setDestVersion(Integer destVersion) {
        this.destVersion = destVersion;
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

    public boolean isBumpupVersion() {
        return bumpupVersion;
    }

    public void setBumpupVersion(boolean bumpupVersion) {
        this.bumpupVersion = bumpupVersion;
    }

}
