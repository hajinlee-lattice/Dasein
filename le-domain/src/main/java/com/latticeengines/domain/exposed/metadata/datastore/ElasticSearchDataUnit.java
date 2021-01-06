package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.TableRoleInCollection;

public class ElasticSearchDataUnit extends DataUnit {

    @JsonProperty("TableRole")
    private TableRoleInCollection tableRole;

    @JsonProperty("Signature")
    private String signature;

    @Override
    public StorageType getStorageType() {
        return StorageType.ElasticSearch;
    }

    public TableRoleInCollection getTableRole() {
        return tableRole;
    }

    public void setTableRole(TableRoleInCollection tableRole) {
        this.tableRole = tableRole;
    }

    public String getSignature() {
        return signature;
    }

    public void setSignature(String signature) {
        this.signature = signature;
    }
}
