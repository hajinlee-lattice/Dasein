package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;

public class ImportConfiguration extends BasePayloadConfiguration {

    private List<SourceImportConfiguration> sourceConfigurations = new ArrayList<>();
    private Map<String, String> properties = new HashMap<>();

    @JsonProperty("sources")
    public List<SourceImportConfiguration> getSourceConfigurations() {
        return sourceConfigurations;
    }

    @JsonProperty("sources")
    public void setSourceConfigurations(List<SourceImportConfiguration> sourceConfigurations) {
        this.sourceConfigurations = sourceConfigurations;
    }

    @JsonIgnore
    public void addSourceConfiguration(SourceImportConfiguration sourceConfiguration) {
        sourceConfigurations.add(sourceConfiguration);
    }

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @JsonIgnore
    public void setProperty(String key, String value) {
        properties.put(key, value);
    }

    @JsonIgnore
    public String getProperty(String key) {
        return properties.get(key);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
