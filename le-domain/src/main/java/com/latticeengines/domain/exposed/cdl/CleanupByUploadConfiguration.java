package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CleanupByUploadConfiguration extends CleanupOperationConfiguration {

    @JsonProperty("file_path")
    private String filePath;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }
}
