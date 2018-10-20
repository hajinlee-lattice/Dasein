package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMCleanerParameters extends TransformationFlowParameters {

    @JsonProperty("AttrOpts")
    private Map<String, CleanOpt> attrOpts;
    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    public static CleanOpt castCleanOpt(String cleanOpt) {
        return CleanOpt.valueOf(cleanOpt);
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public Map<String, CleanOpt> getAttrOpts() {
        return attrOpts;
    }

    public void setAttrOpts(Map<String, CleanOpt> attrOpts) {
        this.attrOpts = attrOpts;
    }

    public enum CleanOpt {
        RETAIN, DROP, LATTICEID, STRING, DOUBLE, INTEGER, LONG, BOOLEAN
    }

}
