package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamoDataUnit extends DataUnit {

    @JsonProperty("Signature")
    private String signature;
    @JsonProperty("PartitionKey")
    private String partitionKey;
    @JsonProperty("SortKey")
    private String sortKey;
    // tenant id used in dynamo pk (optional)
    @JsonProperty("LinkedTenant")
    private String linkedTenant;
    // table name used in dynamo pk (optional)
    @JsonProperty("LinkedTable")
    private String linkedTable;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.Dynamo;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
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

    public String getLinkedTenant() {
        return linkedTenant;
    }

    public void setLinkedTenant(String linkedTenant) {
        this.linkedTenant = linkedTenant;
    }

    public String getLinkedTable() {
        return linkedTable;
    }

    public void setLinkedTable(String linkedTable) {
        this.linkedTable = linkedTable;
    }
}
