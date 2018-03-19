package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class BucketedScore implements Serializable {

    private static final long serialVersionUID = -6994756387548943771L;
    private int score;
    private int numLeads;
    private int numConverted;
    private int leftNumLeads;
    private int leftNumConverted;

    public BucketedScore() {
    }

    public BucketedScore(int score, int numLeads, int numConverted, int leftNumLeads, int leftNumConverted) {
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
