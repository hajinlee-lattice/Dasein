package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamFieldToFilter implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("compare_type")
    private CompareType compareType;

    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("column_value")
    private String columnValue;

    public CompareType getCompareType() {
        return compareType;
    }

    public void setCompareType(CompareType compareType) {
        this.compareType = compareType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnValue() {
        return columnValue;
    }

    public void setColumnValue(String columnValue) {
        this.columnValue = columnValue;
    }

    public enum CompareType {
        Equal, Contains
    }
}
