package com.latticeengines.domain.exposed.spark.common;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ConvertToCSVConfig extends CSVJobConfigBase {

    public static final String NAME = "convertToCSV";
    public static final String TIME_ZONE = "UTC";
    public static final String ISO_8601 = "yyyy-MM-dd'T'HH:mm:ss'Z'"; // default date format

    @JsonProperty("ExportTimeAttr")
    private String exportTimeAttr;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public String getExportTimeAttr() {
        return exportTimeAttr;
    }

    public void setExportTimeAttr(String exportTimeAttr) {
        this.exportTimeAttr = exportTimeAttr;
    }
}
