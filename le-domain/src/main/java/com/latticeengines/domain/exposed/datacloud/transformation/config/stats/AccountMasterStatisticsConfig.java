package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

public class AccountMasterStatisticsConfig extends TransformerConfig {
    private Map<String, String> attributeCategoryMap;

    private Map<String, Map<String, Long>> dimensionValuesIdMap;

    private String cubeColumnName;

    private List<String> dimensions;

    private List<String> specialColumns;

    private boolean numericalBucketsRequired;

    private String dataCloudVersion;

    public Map<String, String> getAttributeCategoryMap() {
        return attributeCategoryMap;
    }

    public void setAttributeCategoryMap(Map<String, String> attributeCategoryMap) {
        this.attributeCategoryMap = attributeCategoryMap;
    }

    public Map<String, Map<String, Long>> getDimensionValuesIdMap() {
        return dimensionValuesIdMap;
    }

    public void setDimensionValuesIdMap(Map<String, Map<String, Long>> dimensionValuesIdMap) {
        this.dimensionValuesIdMap = dimensionValuesIdMap;
    }

    public String getCubeColumnName() {
        return cubeColumnName;
    }

    public void setCubeColumnName(String cubeColumnName) {
        this.cubeColumnName = cubeColumnName;
    }

    public List<String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(List<String> dimensions) {
        this.dimensions = dimensions;
    }

    public List<String> getSpecialColumns() {
        return specialColumns;
    }

    public void setSpecialColumns(List<String> specialColumns) {
        this.specialColumns = specialColumns;
    }

    public boolean isNumericalBucketsRequired() {
        return numericalBucketsRequired;
    }

    public void setNumericalBucketsRequired(boolean numericalBucketsRequired) {
        this.numericalBucketsRequired = numericalBucketsRequired;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

}
