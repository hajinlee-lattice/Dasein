package com.latticeengines.domain.exposed.eai;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.dataplatform.HasProperty;

public class Attribute implements HasName, HasProperty, Serializable {

    private static final long serialVersionUID = -4779448415471374224L;

    private String name;
    private String displayName;
    private Integer length;
    private Boolean nullable = Boolean.FALSE;
    private String physicalDataType;
    private String logicalDataType;
    private Integer precision;
    private Integer scale;
    private List<String> cleanedUpEnumValues = new ArrayList<String>();
    private List<String> enumValues = new ArrayList<String>();
    private Map<String, Object> properties = new HashMap<>();

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    @JsonIgnore
    public String getDisplayName() {
        return displayName;
    }

    @JsonIgnore
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @JsonIgnore
    public Integer getLength() {
        return length;
    }

    @JsonIgnore
    public void setLength(Integer length) {
        this.length = length;
    }

    @JsonIgnore
    public Boolean isNullable() {
        return nullable;
    }

    @JsonIgnore
    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    @JsonIgnore
    public String getPhysicalDataType() {
        return physicalDataType;
    }

    @JsonIgnore
    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public String getLogicalDataType() {
        return logicalDataType;
    }

    public void setLogicalDataType(String logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    @JsonIgnore
    public Integer getPrecision() {
        return precision;
    }

    @JsonIgnore
    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    @JsonIgnore
    public Integer getScale() {
        return scale;
    }

    @JsonIgnore
    public void setScale(Integer scale) {
        this.scale = scale;
    }

    @JsonIgnore
    public List<String> getEnumValues() {
        return enumValues;
    }

    @JsonIgnore
    public void setEnumValues(List<String> enumValues) {
        this.enumValues = enumValues;
    }

    @JsonIgnore
    public List<String> getCleanedUpEnumValues() {
        return cleanedUpEnumValues;
    }

    @JsonIgnore
    public void setCleanedUpEnumValues(List<String> cleanedUpEnumValues) {
        this.cleanedUpEnumValues = cleanedUpEnumValues;
    }

    @JsonIgnore
    @Override
    public Object getPropertyValue(String key) {
        return properties.get(key);
    }

    @JsonIgnore
    @Override
    public void setPropertyValue(String key, Object value) {
        properties.put(key, value);
    }

    @JsonIgnore
    @Override
    public Set<Map.Entry<String, Object>> getEntries() {
        return properties.entrySet();
    }

}
