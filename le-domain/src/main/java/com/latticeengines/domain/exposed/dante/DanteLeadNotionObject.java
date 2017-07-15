package com.latticeengines.domain.exposed.dante;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DanteLeadNotionObject {

    @JsonProperty(value = "ExpectedValue", index = 1)
    private double expectedValue;

    @JsonProperty(value = "ExternalProbability", index = 2)
    private double externalProbability;

    @JsonProperty(value = "LastLaunched", index = 3)
    private Date lastLaunched;

    @JsonProperty(value = "Lift", index = 4)
    private double lift;

    @JsonProperty(value = "LikelihoodBucketDisplayName", index = 5)
    private String likelihoodBucketDisplayName;

    @JsonProperty(value = "LikelihoodBucketOffset", index = 6)
    private int likelihoodBucketOffset;

    @JsonProperty(value = "ModelID", index = 7)
    private String modelId;

    @JsonProperty(value = "Percentile", index = 8)
    private int percentile;

    @JsonProperty(value = "PlayDescription", index = 9)
    private String playDescription;

    @JsonProperty(value = "PlayDisplayName", index = 10)
    private String playDisplayName;

    @JsonProperty(value = "PlayID", index = 11)
    private String playID;

    @JsonProperty(value = "PlaySolutionType", index = 12)
    private String playSolutionType;

    @JsonProperty(value = "PlayTargetProductName", index = 13)
    private String playTargetProductName;

    @JsonProperty(value = "PlayType", index = 14)
    private String playType;

    @JsonProperty(value = "Probability", index = 15)
    private double probability;

    @JsonProperty(value = "Rank", index = 16)
    private int rank;

    @JsonProperty(value = "Theme", index = 17)
    private String theme;

    @JsonProperty(value = "TalkingPoints", index = 18)
    private List<DanteTalkingPoint> talkingPoints;

    public DanteLeadNotionObject() {
    }

    public DanteLeadNotionObject(List<DanteTalkingPoint> danteTalkingPoints) {
        expectedValue = 23456.78;
        externalProbability = 0.70;
        lastLaunched = new Date();
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
        theme = "Theme comes here";
        talkingPoints = danteTalkingPoints;
    }

    public double getExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(double expectedValue) {
        this.expectedValue = expectedValue;
    }

    public double getExternalProbability() {
        return externalProbability;
    }

    public void setExternalProbability(double externalProbability) {
        this.externalProbability = externalProbability;
    }

    public Date getLastLaunched() {
        return lastLaunched;
    }

    public void setLastLaunched(Date lastLaunched) {
        this.lastLaunched = lastLaunched;
    }

    public double getLift() {
        return lift;
    }

    public void setLift(double lift) {
        this.lift = lift;
    }

    public String getLikelihoodBucketDisplayName() {
        return likelihoodBucketDisplayName;
    }

    public void setLikelihoodBucketDisplayName(String likelihoodBucketDisplayName) {
        this.likelihoodBucketDisplayName = likelihoodBucketDisplayName;
    }

    public int getLikelihoodBucketOffset() {
        return likelihoodBucketOffset;
    }

    public void setLikelihoodBucketOffset(int likelihoodBucketOffset) {
        this.likelihoodBucketOffset = likelihoodBucketOffset;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public int getPercentile() {
        return percentile;
    }

    public void setPercentile(int percentile) {
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

    public double getProbability() {
        return probability;
    }

    public void setProbability(double probability) {
        this.probability = probability;
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getTheme() {
        return theme;
    }

    public void setTheme(String theme) {
        this.theme = theme;
    }

    public List<DanteTalkingPoint> getTalkingPoints() {
        return talkingPoints;
    }

    public void setTalkingPoints(List<DanteTalkingPoint> talkingPoints) {
        this.talkingPoints = talkingPoints;
    }
}
