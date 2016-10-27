package com.latticeengines.datacloud.dataflow.refresh;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SeedMergeFieldMapping {
    private String targetColumn;

    private String dnbColumn;

    private String leColumn;

    private ColumnType columnType;

    @JsonProperty("TargetColumn")
    public String getTargetColumn() {
        return targetColumn;
    }

    @JsonProperty("TargetColumn")
    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

    @JsonProperty("DnBColumn")
    public String getDnbColumn() {
        return dnbColumn;
    }

    @JsonProperty("DnBColumn")
    public void setDnbColumn(String dnbColumn) {
        this.dnbColumn = dnbColumn;
    }

    @JsonProperty("LeColumn")
    public String getLeColumn() {
        return leColumn;
    }

    @JsonProperty("LeColumn")
    public void setLeColumn(String leColumn) {
        this.leColumn = leColumn;
    }

    @JsonProperty("ColumnType")
    public ColumnType getColumnType() {
        return columnType;
    }

    @JsonProperty("ColumnType")
    public void setColumnType(ColumnType columnType) {
        this.columnType = columnType;
    }

    public enum ColumnType {
        DOMAIN, DUNS, IS_PRIMARY_DOMAIN, IS_PRIMARY_LOCATION, NUMBER_OF_LOCATION, OTHER
    }
}
