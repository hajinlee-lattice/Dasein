package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class VdbGetQueryData {

    @JsonProperty("vdb_query_handle")
    private String vdbQueryHandle;

    @JsonProperty("start_row")
    private int startRow;

    @JsonProperty("rows_to_get")
    private int rowsToGet;

    public String getVdbQueryHandle() {
        return vdbQueryHandle;
    }

    public void setVdbQueryHandle(String vdbQueryHandle) {
        this.vdbQueryHandle = vdbQueryHandle;
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public int getRowsToGet() {
        return rowsToGet;
    }

    public void setRowsToGet(int rowsToGet) {
        this.rowsToGet = rowsToGet;
    }
}
