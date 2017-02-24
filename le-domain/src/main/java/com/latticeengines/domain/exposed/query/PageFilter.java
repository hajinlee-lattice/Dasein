package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PageFilter {
    @JsonProperty("row_offset")
    private int rowOffset;
    @JsonProperty("num_rows")
    private int numRows;

    public int getRowOffset() {
        return rowOffset;
    }

    public void setRowOffset(int rowOffset) {
        this.rowOffset = rowOffset;
    }

    public int getNumRows() {
        return numRows;
    }

    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }
}
