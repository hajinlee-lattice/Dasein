package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class BucketedScore implements Serializable {

    private static final long serialVersionUID = -6994756387548943771L;
    private int score;
    private int numLeads;
    private double numConverted;
    private int leftNumLeads;
    private double leftNumConverted;

    public BucketedScore() {
    }

    public BucketedScore(int score, int numLeads, double numConverted, int leftNumLeads, double leftNumConverted) {
        this.score = score;
        this.numLeads = numLeads;
        this.numConverted = numConverted;
        this.leftNumLeads = leftNumLeads;
        this.leftNumConverted = leftNumConverted;
    }

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
    public double getNumConverted() {
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
    public double getLeftNumConverted() {
        return leftNumConverted;
    }

    public void setLeftNumConverted(int leftNumConverted) {
        this.leftNumConverted = leftNumConverted;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
