package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class CSVImportFileInfo {

    @JsonProperty("file_upload_initiator")
    private String fileUploadInitiator;

    @JsonProperty("report_file_name")
    private String reportFileName;

    @JsonProperty("report_file_display_name")
    private String reportFileDisplayName;

    @JsonProperty("report_file_path")
    private String reportFilePath;

    @JsonProperty("partial_file")
    private boolean partialFile = false;

    public String getFileUploadInitiator() {
        return this.fileUploadInitiator;
    }

    public void setFileUploadInitiator(String initiator) {
        this.fileUploadInitiator = initiator;
    }

    public String getReportFileName() {
        return reportFileName;
    }

    public void setReportFileName(String reportFileName) {
        this.reportFileName = reportFileName;
    }

    public String getReportFileDisplayName() {
        return reportFileDisplayName;
    }

    public void setReportFileDisplayName(String reportFileDisplayName) {
        this.reportFileDisplayName = reportFileDisplayName;
    }

    public String getReportFilePath() {
        return reportFilePath;
    }

    public void setReportFilePath(String reportFilePath) {
        this.reportFilePath = reportFilePath;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public boolean isPartialFile() {
        return partialFile;
    }

    public void setPartialFile(boolean partialFile) {
        this.partialFile = partialFile;
    }
}
