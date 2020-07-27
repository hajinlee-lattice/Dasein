package com.latticeengines.domain.exposed.dcp;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DunsCountCache {

    @JsonProperty("snapshotTimestamp")
    private Date snapshotTimestamp;

    @JsonProperty("dunsCountTableName")
    private String dunsCountTableName;

    public Date getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    public void setSnapshotTimestamp(Date snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }

    public String getDunsCountTableName() {
        return dunsCountTableName;
    }

    public void setDunsCountTableName(String dunsCountTableName) {
        this.dunsCountTableName = dunsCountTableName;
    }
}
