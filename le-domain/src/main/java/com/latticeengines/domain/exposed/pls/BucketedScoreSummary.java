package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketedScoreSummary {

    private int totalNumLeads;
    private int totalNumConverted;
    private double[] barLifts = new double[32];
    private BucketedScore[] bucketedScores = new BucketedScore[100];

    @JsonProperty("total_num_leads")
    public int getTotalNumLeads() {
        return totalNumLeads;
    }

    public void setTotalNumLeads(int totalNumLeads) {
        this.totalNumLeads = totalNumLeads;
    }

    @JsonProperty("total_num_converted")
    public int getTotalNumConverted() {
        return totalNumConverted;
    }

    public void setTotalNumConverted(int totalNumConverted) {
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
