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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
