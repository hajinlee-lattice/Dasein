package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrbCacheSeedSecondaryDomainAccumulationTransformerConfig extends TransformerConfig {

    @JsonProperty("MarkerFieldName")
    private String markerFieldName;

    @JsonProperty("SecondaryDomainFieldName")
    private String secondaryDomainFieldName;

    @JsonProperty("RenamedSecondaryDomainFieldName")
    private String renamedSecondaryDomainFieldName;

    @JsonProperty("DomainMappingFields")
    private List<String> domainMappingFields;

    public List<String> getDomainMappingFields() {
        return domainMappingFields;
    }

    public void setDomainMappingFields(List<String> domainMappingFields) {
        this.domainMappingFields = domainMappingFields;
    }

    public String getMarkerFieldName() {
        return markerFieldName;
    }

    public void setMarkerFieldName(String markerFieldName) {
        this.markerFieldName = markerFieldName;
    }

    public String getSecondaryDomainFieldName() {
        return secondaryDomainFieldName;
    }

    public void setSecondaryDomainFieldName(String secondaryDomainFieldName) {
        this.secondaryDomainFieldName = secondaryDomainFieldName;
    }

    public String getRenamedSecondaryDomainFieldName() {
        return renamedSecondaryDomainFieldName;
    }

    public void setRenamedSecondaryDomainFieldName(String renamedSecondaryDomainFieldName) {
        this.renamedSecondaryDomainFieldName = renamedSecondaryDomainFieldName;
    }

}
