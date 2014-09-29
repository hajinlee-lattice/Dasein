package com.latticeengines.domain.exposed.eai;

import java.util.ArrayList;
import java.util.List;

import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Attribute implements HasName {

    private String name;
    private String displayName;
    private Integer length;
    private Boolean nullable = Boolean.FALSE;
    private String physicalDataType;
    private String logicalDataType;
    private Integer precision;
    private Integer scale;
    private List<String> enumValues = new ArrayList<String>();
    

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

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Integer getLength() {
        return length;
    }

    public void setLength(Integer length) {
        this.length = length;
    }

    public Boolean isNullable() {
        return nullable;
    }

    public void setNullable(Boolean nullable) {
        this.nullable = nullable;
    }

    public String getPhysicalDataType() {
        return physicalDataType;
    }

    public void setPhysicalDataType(String physicalDataType) {
        this.physicalDataType = physicalDataType;
    }

    public String getLogicalDataType() {
        return logicalDataType;
    }

    public void setLogicalDataType(String logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

	public List<String> getEnumValues() {
		return enumValues;
	}

	public void setEnumValues(List<String> enumValues) {
		this.enumValues = enumValues;
	}
}
