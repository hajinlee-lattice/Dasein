package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlDestination extends PublicationDestination {

    private String tableName;

    @JsonProperty("DestinationType")
    protected String getDestinationType() { return this.getClass().getSimpleName(); }

    @JsonProperty("TableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("TableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
