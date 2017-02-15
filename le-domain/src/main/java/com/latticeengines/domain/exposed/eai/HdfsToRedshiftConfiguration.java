package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsToRedshiftConfiguration extends ExportConfiguration {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("concrete_table")
    private boolean concreteTable = false;

    @JsonProperty("append")
    private boolean append = false;

    @JsonProperty("json_path_prefix")
    private String jsonPathPrefix;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public boolean isConcreteTable() {
        return concreteTable;
    }

    public void setConcreteTable(boolean concreteTable) {
        this.concreteTable = concreteTable;
    }

    public boolean isAppend() {
        return append;
    }

    public void setAppend(boolean append) {
        this.append = append;
    }

    public String getJsonPathPrefix() {
        return this.jsonPathPrefix;
    }

    public void setJsonPathPrefix(String jsonPathPrefix) {
        this.jsonPathPrefix = jsonPathPrefix;
    }
}
