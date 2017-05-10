package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SorterParameters extends TransformationFlowParameters {

    // must be a unique and sortable field
    @JsonProperty("sorting_field")
    private String sortingField;

    @JsonProperty("partitions")
    private int partitions = 1;

    @JsonProperty("partition_field")
    private String partitionField;


    public String getSortingField() {
        return sortingField;
    }

    public void setSortingField(String sortingField) {
        this.sortingField = sortingField;
    }

    public int getPartitions() {
        return partitions;
    }

    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    public String getPartitionField() {
        return partitionField;
    }

    public void setPartitionField(String partitionField) {
        this.partitionField = partitionField;
    }

}
