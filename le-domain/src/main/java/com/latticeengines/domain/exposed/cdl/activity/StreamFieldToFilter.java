package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class StreamFieldToFilter implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("comparison_type")
    private ComparisonType comparisonType;

    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("column_value")
    private String columnValue;

    @JsonProperty("column_value_arrs")
    private List<String> columnValueArrs;

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

    public ComparisonType getComparisonType() {
        return comparisonType;
    }

    public void setComparisonType(ComparisonType comparisonType) {
        this.comparisonType = comparisonType;
    }

    public List<String> getColumnValueArrs() {
        return columnValueArrs;
    }

    public void setColumnValueArrs(List<String> columnValueArrs) {
        this.columnValueArrs = columnValueArrs;
    }

    public enum ComparisonType {
        Equal, Like, In, Unlike
    }
}
