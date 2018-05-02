package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class RedshiftDataUnit extends DataUnit {

    @Override
    @JsonIgnore
    public StorageType getStorageType() {
        return StorageType.Redshift;
    }

    @JsonProperty("RedshiftTable")
    private String redshiftTable;

    public String getRedshiftTable() {
        return redshiftTable;
    }

    public void setRedshiftTable(String redshiftTable) {
        this.redshiftTable = redshiftTable;
    }
}
