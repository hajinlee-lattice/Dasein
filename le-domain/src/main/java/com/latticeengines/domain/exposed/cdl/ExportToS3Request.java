package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ExportToS3Request {

    @JsonProperty("Tenants")
    private List<String> tenants = new ArrayList<>();

    @JsonProperty("OnlyAtlas")
    private Boolean onlyAtlas;

    public List<String> getTenants() {
        return tenants;
    }

    public void setTenants(List<String> tenants) {
        this.tenants = tenants;
    }

    public Boolean getOnlyAtlas() {
        return onlyAtlas;
    }

    public void setOnlyAtlas(Boolean onlyAtlas) {
        this.onlyAtlas = onlyAtlas;
    }
}
