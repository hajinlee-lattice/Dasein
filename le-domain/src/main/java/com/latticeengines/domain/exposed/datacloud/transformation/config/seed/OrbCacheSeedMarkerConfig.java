package com.latticeengines.domain.exposed.datacloud.transformation.config.seed;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class OrbCacheSeedMarkerConfig extends TransformerConfig {
    @JsonProperty("MarkerFieldName")
    private String markerFieldName;

    @JsonProperty("FieldsToCheck")
    private List<String> fieldsToCheck;

    public String getMarkerFieldName() {
        return markerFieldName;
    }

    public void setMarkerFieldName(String markerFieldName) {
        this.markerFieldName = markerFieldName;
    }

    public List<String> getFieldsToCheck() {
        return fieldsToCheck;
    }

    public void setFieldsToCheck(List<String> fieldsToCheck) {
        this.fieldsToCheck = fieldsToCheck;
    }
}
