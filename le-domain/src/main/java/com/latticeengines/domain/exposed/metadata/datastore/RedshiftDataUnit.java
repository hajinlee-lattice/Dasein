package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RedshiftDataUnit extends DataUnit {

    @JsonProperty("RedshiftTable")
    private String redshiftTable;

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
}
