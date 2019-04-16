package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SorterConfig extends TransformerConfig {

    @JsonProperty("SortingField")
    private String sortingField;

    @JsonProperty("Partitions")
    private int partitions = 1;

    @JsonProperty("SplittingThreads")
    private int splittingThreads = 2;

    @JsonProperty("SplittingChunkSize")
    private long splittingChunkSize = -1;

    @JsonProperty("CompressResult")
    private Boolean compressResult;

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

    public int getSplittingThreads() {
        return splittingThreads;
    }

    public void setSplittingThreads(int splittingThreads) {
        this.splittingThreads = splittingThreads;
    }

    public long getSplittingChunkSize() {
        return splittingChunkSize;
    }

    public void setSplittingChunkSize(long splittingChunkSize) {
        this.splittingChunkSize = splittingChunkSize;
    }

    public Boolean getCompressResult() {
        return compressResult;
    }

    public void setCompressResult(Boolean compressResult) {
        this.compressResult = compressResult;
    }
}
