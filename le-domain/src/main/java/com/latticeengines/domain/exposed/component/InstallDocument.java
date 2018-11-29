package com.latticeengines.domain.exposed.component;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class InstallDocument {

    @JsonProperty("data_version")
    private int dataVersion;

    @JsonProperty("version_string")
    private String versionString;

    @JsonProperty("properties_map")
    private Map<String, String> propertiesMap;

    public int getDataVersion() {
        return dataVersion;
    }

    public void setDataVersion(int dataVersion) {
        this.dataVersion = dataVersion;
    }

    public String getVersionString() {
        return versionString;
    }

    public void setVersionString(String versionString) {
        this.versionString = versionString;
    }

    public Map<String, String> getPropertiesMap() {
        return propertiesMap;
    }

    public void setPropertiesMap(Map<String, String> propertiesMap) {
        this.propertiesMap = propertiesMap;
    }

    public void addProperty(String key, String value) {
        if (this.propertiesMap == null) {
            this.propertiesMap = new HashMap<>();
        }
        this.propertiesMap.put(key, value);
    }

    public String getProperty(String key) {
        if (this.propertiesMap == null) {
            return null;
        } else {
            return this.propertiesMap.get(key);
        }
    }
}
