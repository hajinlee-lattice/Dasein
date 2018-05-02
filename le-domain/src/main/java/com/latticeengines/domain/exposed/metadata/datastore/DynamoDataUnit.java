package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DynamoDataUnit extends DataUnit {

    @Override
    @JsonIgnore
    public StorageType getStorageType() {
        return StorageType.Dynamo;
    }

    @JsonProperty("Signature")
    private String signature;

    @JsonProperty("PartitionKey")
    private String partitionKey;

    @JsonProperty("SortKey")
    private String sortKey;

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
}
