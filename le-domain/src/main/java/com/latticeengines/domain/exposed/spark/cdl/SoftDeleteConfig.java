package com.latticeengines.domain.exposed.spark.cdl;


import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class SoftDeleteConfig extends SparkJobConfig {

    public static final String NAME = "softDelete";

    @JsonProperty("DeleteSourceIdx")
    private Integer deleteSourceIdx;

    @JsonProperty("IdColumn")
    private String idColumn;

    @JsonProperty("PartitionKeys")
    private List<String> partitionKeys;

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

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public void setPartitionKeys(List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
    }
}
