package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.eai.CSVToHdfsConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CSVImportConfig extends CDLImportConfig {

    @JsonProperty("csv_inport_file_info")
    private CSVImportFileInfo csvImportFileInfo;

    @JsonProperty("csv_to_hdfs_configuration")
    private CSVToHdfsConfiguration csvToHdfsConfiguration;

    public CSVImportFileInfo getCSVImportFileInfo() {
        return this.csvImportFileInfo;
    }

    public void setCSVImportFileInfo(CSVImportFileInfo info) {
        this.csvImportFileInfo = info;
    }

    public CSVToHdfsConfiguration getCsvToHdfsConfiguration() {
        return csvToHdfsConfiguration;
    }

    public void setCsvToHdfsConfiguration(CSVToHdfsConfiguration csvToHdfsConfiguration) {
        this.csvToHdfsConfiguration = csvToHdfsConfiguration;
    }
}
