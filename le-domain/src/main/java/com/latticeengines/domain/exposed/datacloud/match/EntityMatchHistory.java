package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EntityMatchHistory {
    @JsonProperty("BusinessEntity")
    private String businessEntity;

    @JsonProperty("Matched")
    private String matched;

    @JsonProperty("EntityId")
    private String entityId;

    @JsonProperty("UserId")
    private String userId;

    @JsonProperty("FullMatchKeyTuple")
    private MatchKeyTuple fullMatchKeyTuple;

    @JsonProperty("MatchedMatchKeyTuple")
    private MatchKeyTuple matchedMatchKeyTuple;

    @JsonProperty("MatchType")
    private String matchType;

    // Lead to Account (l2a) Results when main entity is Contact.
    @JsonProperty("L2AMatched")
    private String l2aMatched;

    @JsonProperty("L2AEntityId")
    private String l2aEntityId;

    @JsonProperty("L2AUserId")
    private String l2aUserId;

    @JsonProperty("L2AFullMatchKeyTuple")
    private MatchKeyTuple l2aFullMatchKeyTuple;

    @JsonProperty("L2AMatchedMatchKeyTuple")
    private MatchKeyTuple l2aMatchedMatchKeyTuple;

    @JsonProperty("L2AMatchType")
    private String l2aMatchType;

    public void setBusinessEntity(String businessEntity) {
        this.businessEntity = businessEntity;
    }

    public String getBusinessEntity() {
        return businessEntity;
    }

    public void setMatched(String matched) {
        this.matched = matched;
    }

    public String getMatched() {
        return matched;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUserId() {
        return userId;
    }

    public void setFullMatchKeyTuple(MatchKeyTuple fullMatchKeyTuple) {
        this.fullMatchKeyTuple = fullMatchKeyTuple;
    }

    public MatchKeyTuple getFullMatchKeyTuple() {
        return fullMatchKeyTuple;
    }

    public void setMatchedMatchKeyTuple(MatchKeyTuple matchedMatchKeyTuple) {
        this.matchedMatchKeyTuple = matchedMatchKeyTuple;
    }

    public MatchKeyTuple getMatchedMatchKeyTuple() {
        return matchedMatchKeyTuple;
    }

    public void setMatchType(EntityMatchType entityMatchType) {
        this.matchType = entityMatchType.name();
    }

    public String getMatchType() {
        return matchType;
    }

    public void setL2aMatched(String l2aMatched) {
        this.l2aMatched = l2aMatched;
    }

    public String getL2aMatched() {
        return l2aMatched;
    }

    public void setL2aEntityId(String l2aEntityId) {
        this.l2aEntityId = l2aEntityId;
    }

    public String getL2aEntityId() {
        return l2aEntityId;
    }

    public void setL2aUserId(String l2aUserId) {
        this.l2aUserId = l2aUserId;
    }

    public String getL2aUserId() {
        return l2aUserId;
    }

    public void setL2aFullMatchKeyTuple(MatchKeyTuple l2aFullMatchKeyTuple) {
        this.l2aFullMatchKeyTuple = l2aFullMatchKeyTuple;
    }

    public MatchKeyTuple getL2aFullMatchKeyTuple() {
        return l2aFullMatchKeyTuple;
    }

    public void setL2aMatchedMatchKeyTuple(MatchKeyTuple l2aMatchedMatchKeyTuple) {
        this.l2aMatchedMatchKeyTuple = l2aMatchedMatchKeyTuple;
    }

    public MatchKeyTuple getL2aMatchedMatchKeyTuple() {
        return l2aMatchedMatchKeyTuple;
    }

    public void setL2aMatchType(EntityMatchType l2aEntityMatchType) {
        this.l2aMatchType = l2aEntityMatchType.name();
    }

    public String getL2aMatchType() {
        return l2aMatchType;
    }
}

