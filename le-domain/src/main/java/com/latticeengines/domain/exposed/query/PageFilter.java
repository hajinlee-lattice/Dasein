package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PageFilter {

    @JsonProperty("row_offset")
    private long rowOffset;

    @JsonProperty("num_rows")
    private long numRows;

    public PageFilter() {
    }

    public PageFilter(long rowOffset, long numRows) {
        this.rowOffset = rowOffset;
        this.numRows = numRows;
    }

    public long getRowOffset() {
        return rowOffset;
    }

    public void setRowOffset(long rowOffset) {
        this.rowOffset = rowOffset;
    }

    public long getNumRows() {
        return numRows;
    }

    public void setNumRows(long numRows) {
        this.numRows = numRows;
    }
}
