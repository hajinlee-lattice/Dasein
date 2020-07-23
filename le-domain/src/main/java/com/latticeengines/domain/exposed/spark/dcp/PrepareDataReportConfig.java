package com.latticeengines.domain.exposed.spark.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class PrepareDataReportConfig extends SparkJobConfig {
    public static final String NAME = "PrepareDataReportConfig";

    @JsonProperty("NumTargets")
    private int numTargets;

    @JsonProperty("MatchedDunsAttr")
    private String matchedDunsAttr;

    @JsonProperty("ClassificationAttr")
    private String classificationAttr;

    @Override
    public String getName() {
        return NAME;
    }

    public void setNumTargets(int numTargets) {
        this.numTargets = numTargets;
    }

    @Override
    public int getNumTargets() {
        return numTargets;
    }

    public String getMatchedDunsAttr() {
        return matchedDunsAttr;
    }

    public void setMatchedDunsAttr(String matchedDunsAttr) {
        this.matchedDunsAttr = matchedDunsAttr;
    }

    public String getClassificationAttr() {
        return classificationAttr;
    }

    public void setClassificationAttr(String classificationAttr) {
        this.classificationAttr = classificationAttr;
    }
}
