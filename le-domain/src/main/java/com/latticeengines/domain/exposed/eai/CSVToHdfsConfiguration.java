package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CSVToHdfsConfiguration extends ImportConfiguration {

    @JsonProperty("file_path")
    private String filePath;

    @JsonProperty("file_display_name")
    private String fileDisplayName;

    @JsonProperty("file_name")
    private String fileName;

    @JsonProperty("file_source")
    private String fileSource;

    @JsonProperty("template_name")
    private String templateName;

    @JsonProperty("job_identifier")
    private String jobIdentifier;

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public String getFileSource() {
        return fileSource;
    }

    public void setFileSource(String fileSource) {
        this.fileSource = fileSource;
    }

    public String getTemplateName() {
        return templateName;
    }

    public void setTemplateName(String templateName) {
        this.templateName = templateName;
    }

    public String getJobIdentifier() {
        return jobIdentifier;
    }

    public void setJobIdentifier(String jobIdentifier) {
        this.jobIdentifier = jobIdentifier;
    }

    public String getFileDisplayName() {
        return this.fileDisplayName;
    }

    public void setFileDisplayName(String fileDisplayName) {
        this.fileDisplayName = fileDisplayName;
    }

    public String getFileName() {
        return this.fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }
}
