package com.latticeengines.domain.exposed.metadata.datastore;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PrestoDataUnit extends DataUnit {

    // (clusterId -> tableName)
    @JsonProperty("PrestoTableNames")
    private Map<String, String> prestoTableNames;

    @Override
    @JsonProperty("StorageType")
    public DataUnit.StorageType getStorageType() {
        return DataUnit.StorageType.Presto;
    }

    public Map<String, String> getPrestoTableNames() {
        return prestoTableNames;
    }

    public void setPrestoTableNames(Map<String, String> prestoTableNames) {
        this.prestoTableNames = prestoTableNames;
    }

    public void addPrestoTableName(String clusterId, String tableName) {
        Map<String, String> tableNames = new HashMap<>();
        if (MapUtils.isNotEmpty(tableNames)) {
            tableNames.putAll(prestoTableNames);
        }
        tableNames.put(clusterId, tableName);
        prestoTableNames = tableNames;
    }

    public String getPrestoTableName(String clusterId) {
        if (MapUtils.isNotEmpty(prestoTableNames)) {
            return prestoTableNames.get(clusterId);
        } else {
            return null;
        }
    }
}
