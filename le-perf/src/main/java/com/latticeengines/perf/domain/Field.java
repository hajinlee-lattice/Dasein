package com.latticeengines.perf.domain;

import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

public class Field implements HasName {

    private String name;
    private String columnName;
    private int sqlType;
    private List<String> typeInfo;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("type")
    public List<String> getType() {
        return typeInfo;
    }

    public void setType(List<String> typeInfo) {
        this.typeInfo = typeInfo;
    }

    @JsonProperty("columnName")
    public String getColumnName() {
        return columnName;
    }

    @JsonProperty("columnName")
    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    @JsonProperty("sqlType")
    public int getSqlType() {
        return sqlType;
    }

    @JsonProperty("sqlType")
    public void setSqlType(int sqlType) {
        this.sqlType = sqlType;
    }

}
