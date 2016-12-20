package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

public class AccountMasterStatisticsConfig extends TransformerConfig {
    private Map<String, String> attributeCategoryMap;

    private Map<String, Map<String, Long>> dimensionValuesIdMap;

    private String cubeColumnName;

    private List<String> dimensions;

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

}
