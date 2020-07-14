package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.ComparisonType;

public class StreamFieldToFilter implements Serializable {

    private static final long serialVersionUID = 0L;

    @JsonProperty("comparison_type")
    private ComparisonType comparisonType;

    @JsonProperty("column_name")
    private InterfaceName columnName;

    @JsonProperty("column_value")
    private String columnValue;

    @JsonProperty("column_values")
    private List<String> columnValues;

    public InterfaceName getColumnName() {
        return columnName;
    }

    public void setColumnName(InterfaceName columnName) {
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

    public List<String> getColumnValues() { return columnValues; }

    public void setColumnValues(List<String> columnValues) {
        this.columnValues = columnValues;
    }
}
