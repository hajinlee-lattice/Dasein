package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ElasticSearchDataUnit extends DataUnit {

    @JsonProperty("Signature")
    private String signature;

    @Override
    public StorageType getStorageType() {
        return StorageType.ElasticSearch;
    }


    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
}
