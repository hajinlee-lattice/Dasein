package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbSpecMetadata {

    @JsonProperty("column_name")
    private String columnName;

    @JsonProperty("display_name")
    private String displayName;

    @JsonProperty("data_type")
    private String dataType;

    @JsonProperty("key_column")
    private boolean keyColumn;

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDataType(String dataType) {
        this.dataType = dataType;
    }

    public String getDataType() {
        return dataType;
    }

    public boolean isKeyColumn() {
        return keyColumn;
    }

    public void setKeyColumn(boolean keyColumn) {
        this.keyColumn = keyColumn;
    }
}
