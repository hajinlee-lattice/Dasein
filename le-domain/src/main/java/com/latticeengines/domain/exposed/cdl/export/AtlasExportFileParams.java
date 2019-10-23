package com.latticeengines.domain.exposed.cdl.export;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;


public class AtlasExportFileParams {

    @JsonProperty("FileName")
    private String fileName;

    @JsonProperty("FilesToDelete")
    private List<String> filesToDelete;

    public List<String> getFilesToDelete() {
        return filesToDelete;
    }

    public void setFilesToDelete(List<String> filesToDelete) {
        this.filesToDelete = filesToDelete;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

}
