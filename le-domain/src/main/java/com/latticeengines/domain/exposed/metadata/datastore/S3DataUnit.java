package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class S3DataUnit extends DataUnit {

    @Override
    @JsonIgnore
    public StorageType getStorageType() {
        return StorageType.S3;
    }

    @JsonProperty("LinkedDir")
    private String linkedDir;

    public String getLinkedDir() {
        return linkedDir;
    }

    public void setLinkedDir(String linkedDir) {
        this.linkedDir = linkedDir;
    }

}
