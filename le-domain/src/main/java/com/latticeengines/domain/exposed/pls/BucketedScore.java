package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class BucketedScore {

    private int score;
    private int numLeads;
    private int numConverted;
    private int leftNumLeads;
    private int leftNumConverted;

    @JsonProperty("score")
    public int getScore() {
        return score;
    }

    public void setScore(int score) {
        this.score = score;
    }

    @JsonProperty("num_leads")
    public int getNumLeads() {
        return numLeads;
    }

    public void setNumLeads(int numLeads) {
        this.numLeads = numLeads;
    }

    @JsonProperty("num_converted")
    public int getNumConverted() {
        return numConverted;
    }

    public void setNumConverted(int numConverted) {
        this.numConverted = numConverted;
    }

    @JsonProperty("left_num_leads")
    public int getLeftNumLeads() {
        return leftNumLeads;
    }

    public void setLeftNumLeads(int leftNumLeads) {
        this.leftNumLeads = leftNumLeads;
    }

    @JsonProperty("left_num_converted")
    public int getLeftNumConverted() {
        return leftNumConverted;
    }

    public void setLeftNumConverted(int leftNumConverted) {
        this.leftNumConverted = leftNumConverted;
    }

    public BucketedScore(int score, int numLeads, int numConverted, int leftNumLeads, int leftNumConverted) {
        this.score = score;
        this.numLeads = numLeads;
        this.numConverted = numConverted;
        this.leftNumLeads = leftNumLeads;
        this.leftNumConverted = leftNumConverted;
    }

}
