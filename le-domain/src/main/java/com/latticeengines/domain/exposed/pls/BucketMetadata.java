package com.latticeengines.domain.exposed.pls;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasPid;

// @Table(name = "BUCKET_METADATA", uniqueConstraints = {
        // @UniqueConstraint(columnNames = { "ID", "MODEL_SUMMARY_ID" }) })
@Table(name = "BUCKET_METADATA")
@Entity
public class BucketMetadata implements HasPid, HasId<String> {

    private Long pid;
    private String id;
    private ModelSummary modelSummary;
    private BucketName bucketName;
    private int leftBoundScore;
    private int rightBoundScore;
    private int numLeads;
    private double lift;
    private long creationTimestamp;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonProperty("id")
    @Column(name = "ID", unique = true, nullable = false)
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    @ManyToOne
    @JoinColumn(name = "MODEL_SUMMARY_ID", nullable = false)
    @JsonIgnore
    public ModelSummary getModelSummary() {
        return this.modelSummary;
    }

    @JsonIgnore
    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    @JsonProperty("name")
    @Column(name = "NAME", nullable = false)
    public BucketName getBucketName() {
        return bucketName;
    }

    public void setBucketName(BucketName bucketName) {
        this.bucketName = bucketName;
    }

    @JsonProperty("left_bound_score")
    @Column(name = "LEFT_BOUND_SCORE", nullable = false)
    public int getLeftBoundScore() {
        return leftBoundScore;
    }

    public void setLeftBoundScore(int leftBoundScore) {
        this.leftBoundScore = leftBoundScore;
    }

    @JsonProperty("right_bound_score")
    @Column(name = "RIGHT_BOUND_SCORE", nullable = false)
    public int getRightBoundScore() {
        return rightBoundScore;
    }

    public void setRightBoundScore(int rightBoundScore) {
        this.rightBoundScore = rightBoundScore;
    }

    @JsonProperty("num_leads")
    @Column(name = "NUM_LEADS", nullable = false)
    public int getNumLeads() {
        return numLeads;
    }

    public void setNumLeads(int numLeads) {
        this.numLeads = numLeads;
    }

    @JsonProperty("lift")
    @Column(name = "LIFT", nullable = false)
    public double getLift() {
        return lift;
    }

    public void setLift(double lift) {
        this.lift = lift;
    }

    @JsonProperty("creation_timestamp")
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

}
