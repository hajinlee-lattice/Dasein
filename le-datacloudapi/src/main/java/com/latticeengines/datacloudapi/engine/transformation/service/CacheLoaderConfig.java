package com.latticeengines.datacloudapi.engine.transformation.service;

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
    @JsonProperty("confidenceCode")
    private Integer confidenceCode;
    @JsonProperty("matchGrade")
    private String matchGrade;

    @JsonProperty("matchGradeField")
    private String matchGradeField;
    @JsonProperty("confidenceCodeField")
    private String confidenceCodeField;

    @JsonProperty("isWhiteCache")
    private boolean isWhiteCache = true;

    @JsonProperty("dataCloudVersion")
    private String dataCloudVersion;

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

    public Integer getConfidenceCode() {
        return this.confidenceCode;
    }

    public String getMatchGrade() {
        return this.matchGrade;
    }

    public void setConfidenceCode(Integer confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    public void setMatchGrade(String matchGrade) {
        this.matchGrade = matchGrade;
    }

    public boolean isWhiteCache() {
        return isWhiteCache;
    }

    public void setIsWhiteCache(boolean isWhiteCache) {
        this.isWhiteCache = isWhiteCache;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public String getMatchGradeField() {
        return matchGradeField;
    }

    public void setMatchGradeField(String matchGradeField) {
        this.matchGradeField = matchGradeField;
    }

    public String getConfidenceCodeField() {
        return confidenceCodeField;
    }

    public void setConfidenceCodeField(String confidenceCodeField) {
        this.confidenceCodeField = confidenceCodeField;
    }

    public void setWhiteCache(boolean isWhiteCache) {
        this.isWhiteCache = isWhiteCache;
    }

    @Override
    public String toString() {
        return "CacheLoaderConfig [sourceName=" + sourceName + ", version=" + version + ", dirPath=" + dirPath
                + ", fieldMap=" + fieldMap + ", dunsField=" + dunsField + ", confidenceCode=" + confidenceCode
                + ", matchGrade=" + matchGrade + ", matchGradeField=" + matchGradeField + ", confidenceCodeField="
                + confidenceCodeField + ", isWhiteCache=" + isWhiteCache + ", dataCloudVersion=" + dataCloudVersion
                + "]";
    }

}
