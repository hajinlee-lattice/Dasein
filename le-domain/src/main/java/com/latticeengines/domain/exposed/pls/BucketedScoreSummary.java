package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToOne;
import javax.persistence.Table;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;
import org.hibernate.annotations.Type;
import org.hibernate.annotations.TypeDef;
import org.hibernate.annotations.TypeDefs;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.vladmihalcea.hibernate.type.json.JsonBinaryType;
import com.vladmihalcea.hibernate.type.json.JsonStringType;

@Table(name = "BUCKETED_SCORE_SUMMARY")
@Entity
@JsonIgnoreProperties(ignoreUnknown = true)
@TypeDefs({ @TypeDef(name = "json", typeClass = JsonStringType.class),
        @TypeDef(name = "jsonb", typeClass = JsonBinaryType.class) })
public class BucketedScoreSummary implements HasPid {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @OneToOne
    @JoinColumn(name = "FK_MODELSUMMARY_ID", nullable = false)
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private ModelSummary modelSummary;

    @JsonProperty("total_num_leads")
    @Column(name = "TOTAL_NUM_LEADS", nullable = false)
    private int totalNumLeads;

    @JsonProperty("total_num_converted")
    @Column(name = "TOTAL_NUM_CONVERTED", nullable = false)
    private double totalNumConverted;

    @JsonProperty("overall_lift")
    @Column(name = "OVERAL_LIFT", nullable = false)
    private double overallLift;

    @JsonProperty("bar_lifts")
    @Column(name = "BAR_LIFTS", nullable = false, columnDefinition = "JSON", precision = 16)
    @Type(type = "json")
    private double[] barLifts = new double[32];

    @JsonProperty("bucketed_scores")
    @Column(name = "BUCKETED_SCORES", nullable = false, columnDefinition = "JSON")
    @Type(type = "json")
    private BucketedScore[] bucketedScores = new BucketedScore[100];

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public int getTotalNumLeads() {
        return totalNumLeads;
    }

    public void setTotalNumLeads(int totalNumLeads) {
        this.totalNumLeads = totalNumLeads;
    }

    public double getTotalNumConverted() {
        return totalNumConverted;
    }

    public void setTotalNumConverted(double totalNumConverted) {
        this.totalNumConverted = totalNumConverted;
    }

    public double getOverallLift() {
        return overallLift;
    }

    public void setOverallLift(double overallLift) {
        this.overallLift = overallLift;
    }

    public double[] getBarLifts() {
        return barLifts;
    }

    public void setBarLifts(double[] barLifts) {
        this.barLifts = barLifts;
    }

    public BucketedScore[] getBucketedScores() {
        return bucketedScores;
    }

    public void setBucketedScores(BucketedScore[] bucketedScores) {
        this.bucketedScores = bucketedScores;
    }

    public ModelSummary getModelSummary() {
        return this.modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
