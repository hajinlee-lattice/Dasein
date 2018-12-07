package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3DataUnit extends DataUnit {

    @JsonProperty("LinkedDir")
    private String linkedDir;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.S3;
    }

    public String getLinkedDir() {
        return linkedDir;
    }

    public void setLinkedDir(String linkedDir) {
        this.linkedDir = linkedDir;
    }

}
