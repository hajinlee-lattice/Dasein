package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EventFieldExtractor implements Serializable {

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

    public static final class Builder {
        private EventFieldExtractor eventFieldExtractor;

        public Builder() {
            eventFieldExtractor = new EventFieldExtractor();
        }

        public Builder withMappingType(MappingType mappingType) {
            eventFieldExtractor.setMappingType(mappingType);
            return this;
        }

        public Builder withMappingValue(String mappingValue) {
            eventFieldExtractor.setMappingValue(mappingValue);
            return this;
        }

        public Builder withMappingMap(Map<String, String> mappingMap) {
            eventFieldExtractor.setMappingMap(mappingMap);
            return this;
        }

        public EventFieldExtractor build() {
            return eventFieldExtractor;
        }

    }
}
