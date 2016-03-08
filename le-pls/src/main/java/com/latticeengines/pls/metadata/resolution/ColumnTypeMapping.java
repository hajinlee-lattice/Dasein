package com.latticeengines.pls.metadata.resolution;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ColumnTypeMapping {
    @JsonProperty
    private String columnName;

    @JsonProperty
    private String columnType;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnType() {
        return columnType;
    }

    public void setColumnType(String columnType) {
        this.columnType = columnType;
    }
}
