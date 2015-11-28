package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TableExtract {

    private String tableName;
    private JobStatus status;
    private long extractedRows;
    private long totalRows;

    @JsonProperty("TableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("TableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("Status")
    public JobStatus getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(JobStatus status) {
        this.status = status;
    }

    @JsonProperty("ExtractedRows")
    public long getExtractedRows() {
        return extractedRows;
    }

    @JsonProperty("ExtractedRows")
    public void setExtractedRows(long extractedRows) {
        this.extractedRows = extractedRows;
    }

    @JsonProperty("TotalRows")
    public long getTotalRows() {
        return totalRows;
    }

    @JsonProperty("TotalRows")
    public void setTotalRows(long totalRows) {
        this.totalRows = totalRows;
    }
}
