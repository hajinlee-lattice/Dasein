package com.latticeengines.domain.exposed.serviceapps.core;

import static com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest.CDL;
import static com.latticeengines.domain.exposed.serviceapps.core.BootstrapRequest.LP;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.serviceapps.cdl.CDLBootstrapRequest;
import com.latticeengines.domain.exposed.serviceapps.lp.LPBootstrapRequest;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "Application")
@JsonSubTypes({ @JsonSubTypes.Type(value = CDLBootstrapRequest.class, name = CDL),
        @JsonSubTypes.Type(value = LPBootstrapRequest.class, name = LP) })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect( //
        fieldVisibility = JsonAutoDetect.Visibility.NONE, //
        getterVisibility = JsonAutoDetect.Visibility.NONE, //
        isGetterVisibility = JsonAutoDetect.Visibility.NONE, //
        setterVisibility = JsonAutoDetect.Visibility.NONE //
)
public abstract class BootstrapRequest {

    public static final String CDL = "cdl";
    public static final String LP = "lp";

    @JsonProperty("TenantId")
    private String tenantId;

    @JsonProperty("Application")
    public abstract String getApplication();

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }
}
