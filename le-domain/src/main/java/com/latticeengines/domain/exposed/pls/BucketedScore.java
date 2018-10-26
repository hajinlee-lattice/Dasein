package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketedScore implements Serializable {

    private static final long serialVersionUID = -6994756387548943771L;
    private Integer score;
    private Integer numLeads;
    private Double numConverted;
    private Integer leftNumLeads;
    private Double leftNumConverted;
    private Double averageExpectedRevenue;
    private Double expectedRevenue;
    private Double leftExpectedRevenue;

    public BucketedScore() {
    }

    public BucketedScore(Integer score, Integer numLeads, Double numConverted, Integer leftNumLeads,
            Double leftNumConverted) {
        this(score, numLeads, numConverted, leftNumLeads, leftNumConverted, null, null, null);
    }

    public BucketedScore(Integer score, Integer numLeads, Double numConverted, Integer leftNumLeads,
            Double leftNumConverted, Double averageExpectedRevenue, Double expectedRevenue,
            Double leftExpectedRevenue) {
        super();
        this.score = score;
        this.numLeads = numLeads;
        this.numConverted = numConverted;
        this.leftNumLeads = leftNumLeads;
        this.leftNumConverted = leftNumConverted;
        this.averageExpectedRevenue = averageExpectedRevenue;
        this.expectedRevenue = expectedRevenue;
        this.leftExpectedRevenue = leftExpectedRevenue;
    }

    @JsonProperty("score")
    public Integer getScore() {
        return score;
    }

    public void setScore(Integer score) {
        this.score = score;
    }

    @JsonProperty("num_leads")
    public Integer getNumLeads() {
        return numLeads;
    }

    public void setNumLeads(Integer numLeads) {
        this.numLeads = numLeads;
    }

    @JsonProperty("num_converted")
    public Double getNumConverted() {
        return numConverted;
    }

    public void setNumConverted(Double numConverted) {
        this.numConverted = numConverted;
    }

    @JsonProperty("left_num_leads")
    public Integer getLeftNumLeads() {
        return leftNumLeads;
    }

    public void setLeftNumLeads(Integer leftNumLeads) {
        this.leftNumLeads = leftNumLeads;
    }

    @JsonProperty("left_num_converted")
    public Double getLeftNumConverted() {
        return leftNumConverted;
    }

    public void setLeftNumConverted(Double leftNumConverted) {
        this.leftNumConverted = leftNumConverted;
    }

    @JsonProperty("avg_expected_revenue")
    public Double getAverageExpectedRevenue() {
        return averageExpectedRevenue;
    }

    public void setAverageExpectedRevenue(Double averageExpectedRevenue) {
        this.averageExpectedRevenue = averageExpectedRevenue;
    }

    @JsonProperty("expected_revenue")
    public Double getExpectedRevenue() {
        return expectedRevenue;
    }

    public void setExpectedRevenue(Double expectedRevenue) {
        this.expectedRevenue = expectedRevenue;
    }

    @JsonProperty("left_expected_revenue")
    public Double getLeftExpectedRevenue() {
        return leftExpectedRevenue;
    }

    public void setLeftExpectedRevenue(Double leftExpectedRevenue) {
        this.leftExpectedRevenue = leftExpectedRevenue;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
