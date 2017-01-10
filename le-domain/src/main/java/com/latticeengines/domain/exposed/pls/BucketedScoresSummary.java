package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.List;

public class BucketedScoresSummary {

    private long totalNumLeads;
    private long totalNumConverted;
    private double[] barLifts = new double[32];
    private BucketedScore[] bucketedScores = new BucketedScore[100];

    @JsonProperty("total_num_leads")
    public long getTotalNumLeads() {
        return totalNumLeads;
    }

    public void setTotalNumLeads(long totalNumLeads) {
        this.totalNumLeads = totalNumLeads;
    }

    @JsonProperty("total_num_converted")
    public long getTotalNumConverted() {
        return totalNumConverted;
    }

    public void setTotalNumConverted(long totalNumConverted) {
        this.totalNumConverted = totalNumConverted;
    }

    @JsonProperty("bar_lifts")
    public double[] getBarLifts() {
        return barLifts;
    }

    public void setBarLifts(double[] barLifts) {
        this.barLifts = barLifts;
    }

    @JsonProperty("bucketed_scores")
    public BucketedScore[] getBucketedScores() {
        return bucketedScores;
    }

    public void setBucketedScores(BucketedScore[] bucketedScores) {
        this.bucketedScores = bucketedScores;
    }

}
