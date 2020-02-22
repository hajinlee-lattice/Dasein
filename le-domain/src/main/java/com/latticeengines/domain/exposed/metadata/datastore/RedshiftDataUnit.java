package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RedshiftDataUnit extends DataUnit {

    @JsonProperty("RedshiftTable")
    private String redshiftTable;

    @JsonProperty("ClusterPartition")
    private String clusterPartition;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.Redshift;
    }

    public String getRedshiftTable() {
        return redshiftTable;
    }

    public void setRedshiftTable(String redshiftTable) {
        this.redshiftTable = redshiftTable;
    }

    public String getClusterPartition() {
        return clusterPartition;
    }

    public void setClusterPartition(String clusterPartition) {
        this.clusterPartition = clusterPartition;
    }
}
