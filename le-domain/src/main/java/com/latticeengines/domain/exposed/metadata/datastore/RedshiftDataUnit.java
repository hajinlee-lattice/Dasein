package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class RedshiftDataUnit extends DataUnit {

    @Override
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
