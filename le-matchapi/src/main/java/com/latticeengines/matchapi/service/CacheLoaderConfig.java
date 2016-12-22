package com.latticeengines.matchapi.service;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CacheLoaderConfig {

    @JsonProperty("sourceName")
    private String sourceName;
    @JsonProperty("version")
    private String version;
    @JsonProperty("dirPath")
    private String dirPath;
    @JsonProperty("fieldMap")
    private Map<String, String> fieldMap;
    @JsonProperty("dunsField")
    private String dunsField;

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public String getSourceName() {
        return this.sourceName;
    }

    public String getDirPath() {
        return this.dirPath;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public void setDirPath(String dirPath) {
        this.dirPath = dirPath;
    }

    public Map<String, String> getFieldMap() {
        return this.fieldMap;
    }

    public void setFieldMap(Map<String, String> fieldMap) {
        this.fieldMap = fieldMap;
    }

    public String getDunsField() {
        return dunsField;
    }

    public void setDunsField(String dunsField) {
        this.dunsField = dunsField;
    }

    @Override
    public String toString() {
        return "CacheLoaderConfig [sourceName=" + sourceName + ", version=" + version + ", dirPath=" + dirPath
                + ", fieldMap=" + fieldMap + ", dunsField=" + dunsField + "]";
    }

}
