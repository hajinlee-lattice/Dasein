package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CSVImportConfig extends CDLImportConfig {

    @JsonProperty("report_file_name")
    private String reportFileName;

    @JsonProperty("report_file_display_name")
    private String reportFileDisplayName;

    @JsonProperty("csv_to_hdfs_configuration")
    private CSVToHdfsConfiguration csvToHdfsConfiguration;

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

    public CSVToHdfsConfiguration getCsvToHdfsConfiguration() {
        return csvToHdfsConfiguration;
    }

    public void setCsvToHdfsConfiguration(CSVToHdfsConfiguration csvToHdfsConfiguration) {
        this.csvToHdfsConfiguration = csvToHdfsConfiguration;
    }
}
