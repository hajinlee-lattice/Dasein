package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EntityMatchHistory {
    @JsonProperty("BusinessEntity")
    private String businessEntity;

    @JsonProperty("EntityMatched")
    private String entityMatched;

    @JsonProperty("EntityId")
    private String entityId;

    @JsonProperty("CustomerEntityId")
    private String customerEntityId;

    @JsonProperty("FullMatchKeyTuple")
    private MatchKeyTuple fullMatchKeyTuple;

    @JsonProperty("MatchedMatchKeyTuple")
    private MatchKeyTuple matchedMatchKeyTuple;

    @JsonProperty("MatchType")
    private String matchType;

    //
    // Lead to Account (l2a) Results when main entity is Contact.
    //
    @JsonProperty("L2AEntityMatched")
    private String l2aEntityMatched;

    @JsonProperty("L2AEntityId")
    private String l2aEntityId;

    @JsonProperty("L2ACustomerEntityId")
    private String l2aCustomerEntityId;

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

    public void setEntityMatched(String entityMatched) {
        this.entityMatched = entityMatched;
    }

    public String getEntityMatched() {
        return entityMatched;
    }

    public void setEntityId(String entityId) {
        this.entityId = entityId;
    }

    public String getEntityId() {
        return entityId;
    }

    public void setCustomerEntityId(String customerEntityId) {
        this.customerEntityId = customerEntityId;
    }

    public String getCustomerEntityId() {
        return customerEntityId;
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

    public void setL2aEntityMatched(String l2aEntityMatched) {
        this.l2aEntityMatched = l2aEntityMatched;
    }

    public String getL2aEntityMatched() {
        return l2aEntityMatched;
    }

    public void setL2aEntityId(String l2aEntityId) {
        this.l2aEntityId = l2aEntityId;
    }

    public String getL2aEntityId() {
        return l2aEntityId;
    }

    public void setL2aCustomerEntityId(String l2aCustomerEntityId) {
        this.l2aCustomerEntityId = l2aCustomerEntityId;
    }

    public String getL2aCustomerEntityId() {
        return l2aCustomerEntityId;
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

