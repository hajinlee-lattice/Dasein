package com.latticeengines.domain.exposed.modeling;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;


public class Field implements HasName {

    private String name;
    private String columnName;
    private int sqlType;
    private List<String> typeInfo;
    private String defaultValue;

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
    
    @JsonProperty("default")
    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }
    
    @JsonProperty("default")
    public String getDefaultValue() {
        return defaultValue;
    }
   

}
