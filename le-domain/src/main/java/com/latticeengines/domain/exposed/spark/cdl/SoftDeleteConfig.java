package com.latticeengines.domain.exposed.spark.cdl;

import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SoftDeleteConfig extends SparkJobConfig {

    public static final String NAME = "softDelete";

    @JsonProperty("DeleteSourceIdx")
    private Integer deleteSourceIdx;

    @JsonProperty("IdColumn")
    private String idColumn;

    @JsonProperty("SourceIdColumn")
    private String sourceIdColumn;

    @JsonProperty("PartitionKeys")
    private List<String> partitionKeys;

    @JsonProperty("NeedPartitionOutput")
    private Boolean needPartitionOutput;

    // time column in source
    @JsonProperty("EventTimeColumn")
    private String eventTimeColumn;

    // column that contains list of time ranges that should be deleted
    @JsonProperty("TimeRangesColumn")
    private String timeRangesColumn;

    // global time ranges to apply to every record for deletion evaluation
    @JsonProperty("TimeRangesToDelete")
    private Set<List<Long>> timeRangesToDelete;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public Integer getDeleteSourceIdx() {
        return deleteSourceIdx;
    }

    public void setDeleteSourceIdx(Integer deleteSourceIdx) {
        this.deleteSourceIdx = deleteSourceIdx;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }

    public String getSourceIdColumn() {
        if (StringUtils.isEmpty(sourceIdColumn)) {
            return idColumn;
        } else {
            return sourceIdColumn;
        }
    }

    public void setSourceIdColumn(String sourceIdColumn) {
        this.sourceIdColumn = sourceIdColumn;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }

    public Boolean getNeedPartitionOutput() {
        return needPartitionOutput;
    }

    public void setNeedPartitionOutput(Boolean needPartitionOutput) {
        this.needPartitionOutput = needPartitionOutput;
    }

    public String getEventTimeColumn() {
        return eventTimeColumn;
    }

    public void setEventTimeColumn(String eventTimeColumn) {
        this.eventTimeColumn = eventTimeColumn;
    }

    public String getTimeRangesColumn() {
        return timeRangesColumn;
    }

    public void setTimeRangesColumn(String timeRangesColumn) {
        this.timeRangesColumn = timeRangesColumn;
    }

    public Set<List<Long>> getTimeRangesToDelete() {
        return timeRangesToDelete;
    }

    public void setTimeRangesToDelete(Set<List<Long>> timeRangesToDelete) {
        this.timeRangesToDelete = timeRangesToDelete;
    }
}
