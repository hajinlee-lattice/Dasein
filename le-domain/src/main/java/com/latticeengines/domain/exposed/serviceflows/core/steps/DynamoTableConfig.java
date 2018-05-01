package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DynamoTableConfig {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("partition_key")
    private String partitionKey;

    @JsonProperty("sort_key")
    private String sortKey;

    private DynamoTableConfig() {
    }

    public DynamoTableConfig(String tableName, String primaryKey) {
        this(tableName, primaryKey, "");
    }

    public DynamoTableConfig(String tableName, String primaryKey, String sortKey) {
        this.tableName = tableName;
        this.partitionKey = primaryKey;
        this.sortKey = sortKey;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public String getSortKey() {
        return sortKey;
    }

    public void setSortKey(String sortKey) {
        this.sortKey = sortKey;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
