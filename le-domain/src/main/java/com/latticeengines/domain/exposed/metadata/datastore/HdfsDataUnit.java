package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HdfsDataUnit extends DataUnit {

    @JsonProperty("Path")
    private String path;

    public static HdfsDataUnit fromPath(String path) {
        HdfsDataUnit dataUnit = new HdfsDataUnit();
        dataUnit.setPath(path);
        return dataUnit;
    }

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.Hdfs;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
