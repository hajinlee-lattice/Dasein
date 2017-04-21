package com.latticeengines.domain.exposed.pls;

import java.io.Serializable;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import org.hibernate.annotations.Filter;
import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.IsUserModifiable;

@Table(name = "BUCKET_METADATA")
@Entity
@Filter(name = "tenantFilter", condition = "TENANT_ID = :tenantFilterId")
@JsonIgnoreProperties(ignoreUnknown = true)
public class BucketMetadata implements HasPid, IsUserModifiable, Serializable {

    private static final long serialVersionUID = 5914215732568807732L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @JsonIgnore
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    private Long pid;

    @ManyToOne
    @JoinColumn(name = "MODEL_ID", nullable = false)
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private ModelSummary modelSummary;

    @JsonProperty("bucket_name")
    @Column(name = "NAME", nullable = false)
    @Enumerated(EnumType.STRING)
    private BucketName bucketName;

    @JsonProperty("left_bound_score")
    @Column(name = "LEFT_BOUND_SCORE", nullable = false)
    private int leftBoundScore;

    @JsonProperty("right_bound_score")
    @Column(name = "RIGHT_BOUND_SCORE", nullable = false)
    private int rightBoundScore;

    @JsonProperty("num_leads")
    @Column(name = "NUM_LEADS", nullable = false)
    private int numLeads;

    @JsonProperty("lift")
    @Column(name = "LIFT", nullable = false)
    private double lift;

    @JsonProperty("creation_timestamp")
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    private long creationTimestamp;

    @JsonProperty("last_modified_by_user")
    @Column(name = "LAST_MODIFIED_BY_USER")
    private String lastModifiedByUser;

    @Override
    public Long getPid() {
        return pid;
    }

    @Override
    public void setPid(Long pid) {
        this.pid = pid;
    }

    public ModelSummary getModelSummary() {
        return this.modelSummary;
    }

    public void setModelSummary(ModelSummary modelSummary) {
        this.modelSummary = modelSummary;
    }

    public String getBucketName() {
        return bucketName.toValue();
    }

    @JsonIgnore
    public BucketName getBucket() {
        return bucketName;
    }

    public void setBucket(BucketName bucketName) {
        this.bucketName = bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = BucketName.fromValue(bucketName);
    }

    public int getLeftBoundScore() {
        return leftBoundScore;
    }

    public void setLeftBoundScore(int leftBoundScore) {
        this.leftBoundScore = leftBoundScore;
    }

    public int getRightBoundScore() {
        return rightBoundScore;
    }

    public void setRightBoundScore(int rightBoundScore) {
        this.rightBoundScore = rightBoundScore;
    }

    public int getNumLeads() {
        return numLeads;
    }

    public void setNumLeads(int numLeads) {
        this.numLeads = numLeads;
    }

    public double getLift() {
        return lift;
    }

    public void setLift(double lift) {
        this.lift = lift;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    @Override
    public String getLastModifiedByUser() {
        return lastModifiedByUser;
    }

    @Override
    public void setLastModifiedByUser(String lastModifiedByUser) {
        this.lastModifiedByUser = lastModifiedByUser;
    }

}
