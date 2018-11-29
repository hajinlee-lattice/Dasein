package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public class DanteLeadNotionObject {

    @JsonProperty(value = "BaseExternalID", index = 1)
    private String baseExternalId;

    @JsonProperty(value = "NotionName", index = 2)
    private String notionName;

    @JsonProperty(value = "AnalyticAttributes", index = 3)
    private Object analyticAttributes;

    @JsonProperty(value = "DisplayName", index = 4)
    private String displayName;

    @JsonProperty(value = "ExpectedValue", index = 5)
    private Double expectedValue;

    @JsonProperty(value = "ExternalProbability", index = 6)
    private Double externalProbability;

    @JsonProperty(value = "LastLaunched", index = 7)
    private Date lastLaunched;

    @JsonProperty(value = "LastModified", index = 8)
    private Date lastModified;

    @JsonProperty(value = "LeadID", index = 9)
    private String leadId;

    @JsonProperty(value = "Lift", index = 10)
    private Double lift;

    @JsonProperty(value = "LikelihoodBucketDisplayName", index = 11)
    private String likelihoodBucketDisplayName;

    @JsonProperty(value = "LikelihoodBucketOffset", index = 12)
    private Integer likelihoodBucketOffset;

    @JsonProperty(value = "ModelID", index = 13)
    private String modelId;

    @JsonProperty(value = "Percentile", index = 14)
    private Integer percentile;

    @JsonProperty(value = "PlayDescription", index = 15)
    private String playDescription;

    @JsonProperty(value = "PlayDisplayName", index = 16)
    private String playDisplayName;

    @JsonProperty(value = "PlayID", index = 17)
    private String playID;

    @JsonProperty(value = "PlaySolutionType", index = 18)
    private String playSolutionType;

    @JsonProperty(value = "PlayTargetProductName", index = 19)
    private String playTargetProductName;

    @JsonProperty(value = "PlayType", index = 20)
    private String playType;

    @JsonProperty(value = "Probability", index = 21)
    private Double probability;

    @JsonProperty(value = "Rank", index = 22)
    private Integer rank;

    @JsonProperty(value = "RecommendationID", index = 23)
    private Integer recommendationID;

    @JsonProperty(value = "SalesforceAccountID", index = 24)
    private String salesforceAccountID;

    @JsonProperty(value = "SfdcID", index = 25)
    private Integer sfdcID;

    @JsonProperty(value = "Theme", index = 26)
    private String theme;

    @JsonProperty(value = "UserRoleDisplayName", index = 27)
    private String userRoleDisplayName;

    @JsonProperty(value = "HeaderName", index = 28)
    private String headerName;

    @JsonProperty(value = "Timestamp", index = 29)
    private Date timestamp;

    @JsonProperty(value = "TalkingPoints", index = 30)
    private List<DanteTalkingPointValue> talkingPoints;

    private String danteLeadNotionName = "DanteLead";

    public DanteLeadNotionObject() {
    }

    public DanteLeadNotionObject(Recommendation recommendation, Play play, PlayLaunch playLaunch) {
        baseExternalId = recommendation.getId();
        notionName = danteLeadNotionName;
        displayName = play.getDisplayName();
        analyticAttributes = null;
        expectedValue = null;
        externalProbability = null;
        lastLaunched = playLaunch.getCreated();
        lastModified = recommendation.getLastUpdatedTimestamp();
        leadId = baseExternalId;
        lift = null;
        likelihoodBucketDisplayName = recommendation.getPriorityDisplayName();
        likelihoodBucketOffset = null;
        modelId = null;
        percentile = null;
        playDescription = play.getDescription();
        playDisplayName = play.getDisplayName();
        playID = play.getName();
        playSolutionType = null;
        playTargetProductName = null;
        playType = null;
        probability = null;
        rank = null;
        recommendationID = null;
        salesforceAccountID = recommendation.getSfdcAccountID();
        theme = null;
        talkingPoints = null;
        userRoleDisplayName = null;
        headerName = null;
        timestamp = lastModified;
    }

    public DanteLeadNotionObject(List<DanteTalkingPointValue> danteTalkingPoints) {
        baseExternalId = "testLeadForPreview";
        notionName = danteLeadNotionName;
        displayName = "Demo Lead for Preview";
        analyticAttributes = null;
        expectedValue = 23456.78;
        externalProbability = 0.70;
        lastLaunched = new Date();
        lastModified = lastLaunched;
        leadId = baseExternalId;
        lift = 2.3;
        likelihoodBucketDisplayName = "A";
        likelihoodBucketOffset = 3;
        modelId = UUID.randomUUID().toString();
        percentile = 83;
        playDescription = "Play Description comes here";
        playDisplayName = "Play DisplayName";
        playID = "play External ID";
        playSolutionType = "PlaySolutionType";
        playTargetProductName = "TargetProductName";
        playType = "PlayType";
        probability = 0.70;
        rank = 2;
        recommendationID = 42;
        theme = "Theme comes here";
        talkingPoints = danteTalkingPoints;
        userRoleDisplayName = "Play Creator";
        headerName = "Some Header";
        timestamp = lastLaunched;
    }

    public Double getExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(Double expectedValue) {
        this.expectedValue = expectedValue;
    }

    public Double getExternalProbability() {
        return externalProbability;
    }

    public void setExternalProbability(Double externalProbability) {
        this.externalProbability = externalProbability;
    }

    public Date getLastLaunched() {
        return lastLaunched;
    }

    public void setLastLaunched(Date lastLaunched) {
        this.lastLaunched = lastLaunched;
    }

    public Double getLift() {
        return lift;
    }

    public void setLift(Double lift) {
        this.lift = lift;
    }

    public String getLikelihoodBucketDisplayName() {
        return likelihoodBucketDisplayName;
    }

    public void setLikelihoodBucketDisplayName(String likelihoodBucketDisplayName) {
        this.likelihoodBucketDisplayName = likelihoodBucketDisplayName;
    }

    public Integer getLikelihoodBucketOffset() {
        return likelihoodBucketOffset;
    }

    public void setLikelihoodBucketOffset(Integer likelihoodBucketOffset) {
        this.likelihoodBucketOffset = likelihoodBucketOffset;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public Integer getPercentile() {
        return percentile;
    }

    public void setPercentile(Integer percentile) {
        this.percentile = percentile;
    }

    public String getPlayDescription() {
        return playDescription;
    }

    public void setPlayDescription(String playDescription) {
        this.playDescription = playDescription;
    }

    public String getPlayDisplayName() {
        return playDisplayName;
    }

    public void setPlayDisplayName(String playDisplayName) {
        this.playDisplayName = playDisplayName;
    }

    public String getPlayID() {
        return playID;
    }

    public void setPlayID(String playID) {
        this.playID = playID;
    }

    public String getPlaySolutionType() {
        return playSolutionType;
    }

    public void setPlaySolutionType(String playSolutionType) {
        this.playSolutionType = playSolutionType;
    }

    public String getPlayTargetProductName() {
        return playTargetProductName;
    }

    public void setPlayTargetProductName(String playTargetProductName) {
        this.playTargetProductName = playTargetProductName;
    }

    public String getPlayType() {
        return playType;
    }

    public void setPlayType(String playType) {
        this.playType = playType;
    }

    public Double getProbability() {
        return probability;
    }

    public void setProbability(Double probability) {
        this.probability = probability;
    }

    public Integer getRank() {
        return rank;
    }

    public void setRank(Integer rank) {
        this.rank = rank;
    }

    public String getTheme() {
        return theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public String getBaseExternalId() {
        return baseExternalId;
    }

    public void setBaseExternalId(String baseExternalId) {
        this.baseExternalId = baseExternalId;
    }

    public String getNotionName() {
        return notionName;
    }

    public void setNotionName(String notionName) {
        this.notionName = notionName;
    }

    public Object getAnalyticAttributes() {
        return analyticAttributes;
    }

    public void setAnalyticAttributes(Object analyticAttributes) {
        this.analyticAttributes = analyticAttributes;
    }

    public String getDisplayName() {
        return displayName;
    }

    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    public Date getLastModified() {
        return lastModified;
    }

    public void setLastModified(Date lastModified) {
        this.lastModified = lastModified;
    }

    public String getLeadId() {
        return leadId;
    }

    public void setLeadId(String leadId) {
        this.leadId = leadId;
    }

    public Integer getRecommendationID() {
        return recommendationID;
    }

    public void setRecommendationID(Integer recommendationID) {
        this.recommendationID = recommendationID;
    }

    public String getSalesforceAccountID() {
        return salesforceAccountID;
    }

    public void setSalesforceAccountID(String salesforceAccountID) {
        this.salesforceAccountID = salesforceAccountID;
    }

    public Integer getSfdcID() {
        return sfdcID;
    }

    public void setSfdcID(Integer sfdcID) {
        this.sfdcID = sfdcID;
    }

    public String getUserRoleDisplayName() {
        return userRoleDisplayName;
    }

    public void setUserRoleDisplayName(String userRoleDisplayName) {
        this.userRoleDisplayName = userRoleDisplayName;
    }

    public String getHeaderName() {
        return headerName;
    }

    public void setHeaderName(String headerName) {
        this.headerName = headerName;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public List<DanteTalkingPointValue> getTalkingPoints() {
        return talkingPoints;
    }

    public void setTalkingPoints(List<DanteTalkingPointValue> talkingPoints) {
        this.talkingPoints = talkingPoints;
    }
}
