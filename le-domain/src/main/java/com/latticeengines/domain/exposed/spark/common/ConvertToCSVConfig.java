package com.latticeengines.domain.exposed.spark.common;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ConvertToCSVConfig extends SparkJobConfig {

    public static final String NAME = "convertToCSV";
    public static final String ISO_8601 = "yyyy-MM-dd'T'HH:mm:ss'Z'"; // default date format

    @JsonProperty("DisplayNames")
    private Map<String, String> displayNames;

    // format of date attrs. Java SimpleDateFormat in UTC
    // if not specified here, date attr will output as long
    @JsonProperty("DateAttrsFmt")
    private Map<String, String> dateAttrsFmt;

    // a value can be used by TimeZone.getTimeZone. Default is UTC
    @JsonProperty("TimeZone")
    private String timeZone;

    @JsonProperty("Compress")
    private Boolean compress;

    @JsonProperty("ExportTimeAttr")
    private String exportTimeAttr;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public Map<String, String> getDisplayNames() {
        return displayNames;
    }

    public void setDisplayNames(Map<String, String> displayNames) {
        this.displayNames = displayNames;
    }

    public Map<String, String> getDateAttrsFmt() {
        return dateAttrsFmt;
    }

    public void setDateAttrsFmt(Map<String, String> dateAttrsFmt) {
        this.dateAttrsFmt = dateAttrsFmt;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public Boolean getCompress() {
        return compress;
    }

    public void setCompress(Boolean compress) {
        this.compress = compress;
    }

    public String getExportTimeAttr() {
        return exportTimeAttr;
    }

    public void setExportTimeAttr(String exportTimeAttr) {
        this.exportTimeAttr = exportTimeAttr;
    }
}
