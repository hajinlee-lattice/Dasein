package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class VdbQueryResultColumn {

    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("data_type")
    private String dataType;

    @JsonProperty("data_length")
    private short dataLength;

    @JsonProperty("values")
    private List<String> values;

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getDataType() {
        return dataType;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public short getDataLength() {
        return dataLength;
    }

    public void setDataLength(short dataLength) {
        this.dataLength = dataLength;
    }

    public List<String> getValues() {
        return values;
    }

    public void setValues(List<String> values) {
        this.values = values;
    }
}
