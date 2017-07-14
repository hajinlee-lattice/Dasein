package com.latticeengines.dante.metadata;

import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

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

    DanteLeadNotionObject(List<DanteTalkingPoint> danteTalkingPoints) {
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
}
