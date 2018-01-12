package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CleanupByUploadConfiguration extends CleanupOperationConfiguration {

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
