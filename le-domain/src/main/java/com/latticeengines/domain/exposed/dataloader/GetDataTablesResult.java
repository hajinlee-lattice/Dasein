package com.latticeengines.domain.exposed.dataloader;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;

public class GetDataTablesResult {

    @JsonProperty("success")
    private boolean success;

    @JsonProperty("error_message")
    private String errorMessage;

    @JsonProperty("table_config")
    private List<VdbLoadTableConfig> tableConfigs;

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

    public List<VdbLoadTableConfig> getTableConfigs() {
        return tableConfigs;
    }

    public void setTableConfigs(List<VdbLoadTableConfig> tableConfigs) {
        this.tableConfigs = tableConfigs;
    }
}
