package com.latticeengines.domain.exposed.pls;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.cdl.rating.AdvancedRatingConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class RatingEngineSummary {

    @JsonProperty("id")
    private String id;
    @JsonProperty("displayName")
    private String displayName;
    @JsonProperty("note")
    private String note;
    @JsonProperty("type")
    private RatingEngineType type;
    @JsonProperty("status")
    private RatingEngineStatus status = RatingEngineStatus.INACTIVE;
    @JsonProperty("segmentDisplayName")
    private String segmentDisplayName;
    @JsonProperty("segmentName")
    private String segmentName;
    @JsonProperty("created")
    private Date created;
    @JsonProperty("updated")
    private Date updated;
    @JsonProperty("lastRefreshedDate")
    private Date lastRefreshedDate;
    @JsonProperty("createdBy")
    private String createdBy;
    @JsonProperty("updatedBy")
    private String updatedBy;
    @JsonProperty("accountsInSegment")
    private Long accountsInSegment;
    @JsonProperty("contactsInSegment")
    private Long contactsInSegment;
    @JsonProperty("coverage")
    private Map<String, Long> coverage;
    @JsonProperty("latestIterationId")
    private String latestIterationId;
    @JsonProperty("scoringIterationId")
    private String scoringIterationId;
    @JsonProperty("publishedIterationId")
    private String publishedIterationId;
    @JsonProperty("activeModelId")
    private String activeModelId;
    @JsonProperty("bucketMetadata")
    private List<BucketMetadata> bucketMetadata;
    @JsonProperty("advancedRatingConfig")
    private AdvancedRatingConfig advancedRatingConfig;
    @JsonProperty("isPublished")
    private boolean isPublished;
    @JsonProperty("deleted")
    private Boolean deleted;
    @JsonProperty("completed")
    private Boolean completed;

    public RatingEngineSummary() {
    }

    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDisplayName() {
        return this.displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Date getCreated() {
        return this.created;
    }

    public void setCreated(Date time) {
        this.created = time;
    }

    public Date getUpdated() {
        return this.updated;
    }

    public void setUpdated(Date time) {
        this.updated = time;
    }

    public String getNote() {
        return this.note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public RatingEngineType getType() {
        return this.type;
    }

    public void setType(RatingEngineType type) {
        this.type = type;
    }

    public RatingEngineStatus getStatus() {
        return this.status;
    }

    public void setStatus(RatingEngineStatus status) {
        this.status = status;
    }

    public String getSegmentDisplayName() {
        return this.segmentDisplayName;
    }

    public void setSegmentDisplayName(String segmentDisplayName) {
        this.segmentDisplayName = segmentDisplayName;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

    public String getCreatedBy() {
        return this.createdBy;
    }

    public void setCreatedBy(String user) {
        this.createdBy = user;
    }

    public String getUpdatedBy() {
        return this.updatedBy;
    }

    public void setUpdatedBy(String user) {
        this.updatedBy = user;
    }

    public Date getLastRefreshedDate() {
        return this.lastRefreshedDate;
    }

    public void setLastRefreshedDate(Date date) {
        this.lastRefreshedDate = date;
    }

    public Long getAccountsInSegment() {
        return accountsInSegment;
    }

    public void setAccountsInSegment(Long accountsInSegment) {
        this.accountsInSegment = accountsInSegment;
    }

    public Long getContactsInSegment() {
        return contactsInSegment;
    }

    public void setContactsInSegment(Long contactsInSegment) {
        this.contactsInSegment = contactsInSegment;
    }

    public Map<String, Long> getCoverage() {
        return coverage;
    }

    public void setCoverage(Map<String, Long> coverage) {
        this.coverage = coverage;
    }

    public String getActiveModelId() {
        return activeModelId;
    }

    public void setActiveModelId(String activeModelId) {
        this.activeModelId = activeModelId;
    }

    public String getLatestIterationId() {
        return latestIterationId;
    }

    public void setLatestIterationId(String latestIterationId) {
        this.latestIterationId = latestIterationId;
    }

    public String getScoringIterationId() {
        return scoringIterationId;
    }

    public void setScoringIterationId(String scoringIterationId) {
        this.scoringIterationId = scoringIterationId;
    }

    public String getPublishedIterationId() {
        return publishedIterationId;
    }

    public void setPublishedIterationId(String publishedIterationId) {
        this.publishedIterationId = publishedIterationId;
    }

    public List<BucketMetadata> getBucketMetadata() {
        return bucketMetadata;
    }

    public void setBucketMetadata(List<BucketMetadata> bucketMetadata) {
        this.bucketMetadata = bucketMetadata;
    }

    public AdvancedRatingConfig getAdvancedRatingConfig() {
        return advancedRatingConfig;
    }

    public void setAdvancedRatingConfig(AdvancedRatingConfig advancedRatingConfig) {
        this.advancedRatingConfig = advancedRatingConfig;
    }

    public boolean isPublished() {
        return isPublished;
    }

    public void setPublished(boolean published) {
        isPublished = published;
    }

    public Boolean getDeleted() {
        return deleted;
    }

    public void setDeleted(Boolean deleted) {
        this.deleted = deleted;
    }

    public Boolean getCompleted() {
        return completed;
    }

    public void setCompleted(Boolean completed) {
        this.completed = completed;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
