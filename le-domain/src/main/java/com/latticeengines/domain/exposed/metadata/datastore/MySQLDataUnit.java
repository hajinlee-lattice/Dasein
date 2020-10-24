package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MySQLDataUnit extends DataUnit {

    @JsonProperty("MySQLTable")
    private String mySqlTable;

    @Override
    @JsonProperty("StorageType")
    public StorageType getStorageType() {
        return StorageType.MySQL;
    }

    public String getMySqlTable() {
        return mySqlTable;
    }

    public void setMySqlTable(String mySqlTable) {
        this.mySqlTable = mySqlTable;
    }
}
