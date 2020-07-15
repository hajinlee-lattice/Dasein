package com.latticeengines.domain.exposed.dcp;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.Table;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DunsCountCache {

    @JsonProperty("snapshotTimestamp")
    private Date snapshotTimestamp;

    @JsonProperty("dunsCount")
    private Table dunsCount;

    public Date getSnapshotTimestamp() {
        return snapshotTimestamp;
    }

    public void setSnapshotTimestamp(Date snapshotTimestamp) {
        this.snapshotTimestamp = snapshotTimestamp;
    }

    public Table getDunsCount() {
        return dunsCount;
    }

    public void setDunsCount(Table dunsCount) {
        this.dunsCount = dunsCount;
    }

}
