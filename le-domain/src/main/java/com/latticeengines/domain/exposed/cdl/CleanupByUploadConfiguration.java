package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CleanupByUploadConfiguration extends CleanupOperationConfiguration {

    @JsonProperty("table_name")
    private String tableName;

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("file_name")
    private String fileName;

    @JsonProperty("file_display_name")
    private String fileDisplayName;

    @JsonProperty("use_dl_data")
    private boolean useDLData = false;

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

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getFileDisplayName() {
        return this.fileDisplayName;
    }

    public void setFileDisplayName(String displayName) {
        this.fileDisplayName = displayName;
    }

    public boolean isUseDLData() {
        return useDLData;
    }

    public void setUseDLData(boolean useDLData) {
        this.useDLData = useDLData;
    }
}
