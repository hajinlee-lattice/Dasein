package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class DeleteFileUploadStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("file_path")
    private String filePath;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
