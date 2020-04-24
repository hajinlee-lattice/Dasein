package com.latticeengines.domain.exposed.datafabric;

import java.time.Instant;
import java.util.Map;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public class GenericTimeseriesRecord {

    public static final String PARTITION_KEY_ATTR = "PartitionKey";
    public static final String RANGE_KEY_ATTR = "RangeKey";
    public static final String RECORD_ATTR = "RecordKey";

    @NotBlank(message = "Partition Key cannot be empty")
    private String partitionKey;

    @NotNull(message = "Range key timestamp cannot be null")
    private Instant rangeKeyTimestamp;

    @NotBlank(message = "Range key timestamp cannot be null")
    private String rangeKeyID;

    private String rangeKey;

    private Map<String, Object> record;

    public String getPartitionKey() {
        return partitionKey;
    }

    public void setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
    }

    public Instant getRangeKeyTimestamp() {
        return rangeKeyTimestamp;
    }

    public void setRangeKeyTimestamp(Instant rangeKeyTimestamp) {
        this.rangeKeyTimestamp = rangeKeyTimestamp;
    }

    public String getRangeKeyID() {
        return rangeKeyID;
    }

    public void setRangeKeyID(String rangeKeyID) {
        this.rangeKeyID = rangeKeyID;
    }

    public String getRangeKey() {
        return rangeKeyTimestamp.toEpochMilli() + "_" + rangeKeyID;
    }

    public void setRangeKey(String rangeKey) {
        this.rangeKey = rangeKey;
    }

    public Map<String, Object> getRecord() {
        return record;
    }

    public void setRecord(Map<String, Object> record) {
        this.record = record;
    }
}
