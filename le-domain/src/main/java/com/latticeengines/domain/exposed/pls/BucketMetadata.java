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
import javax.persistence.Transient;

import org.hibernate.annotations.OnDelete;
import org.hibernate.annotations.OnDeleteAction;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.IsUserModifiable;

@Table(name = "BUCKET_METADATA")
@Entity
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

    @ManyToOne
    @JoinColumn(name = "RATING_ENGINE_ID")
    @JsonIgnore
    @OnDelete(action = OnDeleteAction.CASCADE)
    private RatingEngine ratingEngine;

    @JsonProperty("bucket_name")
    @Column(name = "NAME", nullable = false)
    @Enumerated(EnumType.STRING)
    private BucketName bucketName;

    @Deprecated // should only use right bound (lower bound)
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

    @JsonProperty("published_version")
    @Column(name = "PUBLISHED_VERSION")
    private Integer publishedVersion;
    
    @JsonProperty("creation_timestamp")
    @Column(name = "CREATION_TIMESTAMP", nullable = false)
    private long creationTimestamp;

    @JsonProperty("orig_creation_timestamp")
    @Column(name = "ORIG_CREATION_TIMESTAMP", nullable = true)
    private Long origCreationTimestamp;

    @JsonProperty("last_modified_by_user")
    @Column(name = "LAST_MODIFIED_BY_USER")
    private String lastModifiedByUser;

    @JsonProperty("avg_expected_revenue")
    @Column(name = "AVG_EXPECTED_REVENUE", nullable = true)
    private Double averageExpectedRevenue;

    @JsonProperty("total_expected_revenue")
    @Column(name = "TOTAL_EXPECTED_REVENUE", nullable = true)
    private Double totalExpectedRevenue;

    @JsonProperty("model_summary_id")
    @Transient
    private String modelSummaryId;
    
    public BucketMetadata() {
    }

    public BucketMetadata(BucketName bucketName, int numleads) {
        this.bucketName = bucketName;
        this.numLeads = numleads;
    }

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

    public void setBucketName(String bucketName) {
        this.bucketName = BucketName.fromValue(bucketName);
    }

    @JsonIgnore
    public BucketName getBucket() {
        return bucketName;
    }

    public void setBucket(BucketName bucketName) {
        this.bucketName = bucketName;
    }

    @Deprecated // should only use right bound (lower bound)
    public int getLeftBoundScore() {
        return leftBoundScore;
    }

    @Deprecated // should only use right bound (lower bound)
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

    public Integer getPublishedVersion() {
        return publishedVersion;
    }

    public void setPublishedVersion(Integer publishedVersion) {
        this.publishedVersion = publishedVersion;
    }

    public Long getOrigCreationTimestamp() {
        return origCreationTimestamp;
    }

    public void setOrigCreationTimestamp(Long origCreationTimestamp) {
        this.origCreationTimestamp = origCreationTimestamp;
    }

    public String getModelSummaryId() {
        return modelSummaryId;
    }

    public void setModelSummaryId(String modelSummaryId) {
        this.modelSummaryId = modelSummaryId;
    }

    public long getCreationTimestamp() {
        return creationTimestamp;
    }

    public void setCreationTimestamp(long creationTimestamp) {
        this.creationTimestamp = creationTimestamp;
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }

    public void setRatingEngine(RatingEngine ratingEngine) {
        this.ratingEngine = ratingEngine;
    }

    @Override
    public String getLastModifiedByUser() {
        return lastModifiedByUser;
    }

    @Override
    public void setLastModifiedByUser(String lastModifiedByUser) {
        this.lastModifiedByUser = lastModifiedByUser;
    }

    public Double getAverageExpectedRevenue() {
        return averageExpectedRevenue;
    }

    public void setAverageExpectedRevenue(Double averageExpectedRevenue) {
        this.averageExpectedRevenue = averageExpectedRevenue;
    }

    public Double getTotalExpectedRevenue() {
        return totalExpectedRevenue;
    }

    public void setTotalExpectedRevenue(Double totalExpectedRevenue) {
        this.totalExpectedRevenue = totalExpectedRevenue;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (obj instanceof BucketMetadata) {
            BucketMetadata bucketMetadata = (BucketMetadata) obj;
            return this.getModelSummary().getId().equals(bucketMetadata.getModelSummary().getId())
                    && this.getRatingEngine().getId().equals(bucketMetadata.getRatingEngine().getId())
                    && this.getBucketName().equals(bucketMetadata.getBucketName())
                    && this.getRightBoundScore() == bucketMetadata.getRightBoundScore()
                    && this.getLift() == bucketMetadata.getLift() && this.getNumLeads() == bucketMetadata.getNumLeads();
        }
        return false;
    }

}
