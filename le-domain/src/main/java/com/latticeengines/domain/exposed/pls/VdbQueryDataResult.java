package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

public class VdbQueryDataResult {

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("columns")
    private List<VdbQueryResultColumn> columns;

    public boolean isSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public List<VdbQueryResultColumn> getColumns() {
        return columns;
    }

    public void setColumns(List<VdbQueryResultColumn> columns) {
        this.columns = columns;
    }
}
