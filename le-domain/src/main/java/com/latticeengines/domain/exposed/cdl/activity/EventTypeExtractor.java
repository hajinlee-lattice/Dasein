package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventTypeExtractor implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("mapping_type")
    private MappingType mappingType;

    /*
     *if mappingType is Attribute or AttributeWithMapping
     * mappingValue will be AttributeName
     * if mappingType is Constant. mappingValue will be a String.
     */
    @JsonProperty("mapping_value")
    private String mappingValue;

    @JsonProperty("mapping_map")
    private Map<String, String> mappingMap;

    public MappingType getMappingType() {
        return mappingType;
    }

    public void setMappingType(MappingType mappingType) {
        this.mappingType = mappingType;
    }

    public String getMappingValue() {
        return mappingValue;
    }

    public void setMappingValue(String mappingValue) {
        this.mappingValue = mappingValue;
    }

    public Map<String, String> getMappingMap() {
        return mappingMap;
    }

    public void setMappingMap(Map<String, String> mappingMap) {
        this.mappingMap = mappingMap;
    }

    public enum MappingType {
        Constant, Attribute, AttributeWithMapping
    }
}
