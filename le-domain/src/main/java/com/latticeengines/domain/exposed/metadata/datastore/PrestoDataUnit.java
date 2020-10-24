package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PrestoDataUnit extends DataUnit {

    @JsonProperty("ClusterId")
    private String clusterId;

    @JsonProperty("PrestoTable")
    private String prestoTable;

    @Override
    @JsonProperty("StorageType")
    public DataUnit.StorageType getStorageType() {
        return DataUnit.StorageType.Presto;
    }

    public String getClusterId() {
        return clusterId;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public String getPrestoTable() {
        return prestoTable;
    }

    public void setPrestoTable(String prestoTable) {
        this.prestoTable = prestoTable;
    }

}
