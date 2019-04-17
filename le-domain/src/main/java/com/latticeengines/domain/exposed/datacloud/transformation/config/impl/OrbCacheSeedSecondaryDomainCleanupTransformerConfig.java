package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class OrbCacheSeedSecondaryDomainCleanupTransformerConfig extends TransformerConfig {

    @JsonProperty("MarkerFieldName")
    private String markerFieldName;

    public String getMarkerFieldName() {
        return markerFieldName;
    }

    public void setMarkerFieldName(String markerFieldName) {
        this.markerFieldName = markerFieldName;
    }
}
