package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsToSnowflakeConfiguration extends ExportConfiguration {

    @JsonProperty("db")
    private String db;

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("concrete_table")
    private boolean concreteTable = false;

    @JsonProperty("append")
    private boolean append = false;

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

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
}
