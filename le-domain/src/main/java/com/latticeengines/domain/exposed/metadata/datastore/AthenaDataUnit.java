package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AthenaDataUnit extends DataUnit {

    @JsonProperty("AthenaTable")
    private String athenaTable;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.Athena;
    }

    public String getAthenaTable() {
        return athenaTable;
    }

    public void setAthenaTable(String athenaTable) {
        this.athenaTable = athenaTable;
    }

}
