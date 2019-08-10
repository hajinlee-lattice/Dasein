package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

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

    @JsonProperty("EntityMatchType")
    private String entityMatchType;

    @JsonProperty("MatchedEntityMatchKeyTuple")
    private MatchKeyTuple matchedEntityMatchKeyTuple;

    @JsonProperty("LdcMatchType")
    private String ldcMatchType;

    @JsonProperty("MatchedLdcMatchKeyTuple")
    private MatchKeyTuple matchedLdcMatchKeyTuple;

    @JsonProperty("ExistingLookupKeyList")
    private List<Pair<String, MatchKeyTuple>> existingLookupKeyList;

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

    @JsonProperty("L2AEntityMatchType")
    private String l2aEntityMatchType;

    @JsonProperty("L2AMatchedEntityMatchKeyTuple")
    private MatchKeyTuple l2aMatchedEntityMatchKeyTuple;

    @JsonProperty("L2ALdcMatchType")
    private String l2aLdcMatchType;

    @JsonProperty("L2AMatchedLdcMatchKeyTuple")
    private MatchKeyTuple l2aMatchedLdcMatchKeyTuple;

    @JsonProperty("L2AExistingLookupKeyList")
    private List<Pair<String, MatchKeyTuple>> l2aExistingLookupKeyList;

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

    public void setEntityMatchType(EntityMatchType entityMatchType) {
        this.entityMatchType = entityMatchType.name();
    }

    public String getEntityMatchType() {
        return entityMatchType;
    }

    public void setMatchedEntityMatchKeyTuple(MatchKeyTuple matchedEntityMatchKeyTuple) {
        this.matchedEntityMatchKeyTuple = matchedEntityMatchKeyTuple;
    }

    public MatchKeyTuple getMatchedEntityMatchKeyTuple() {
        return matchedEntityMatchKeyTuple;
    }

    public void setLdcMatchType(LdcMatchType ldcMatchType) {
        this.ldcMatchType = ldcMatchType.name();
    }

    public String getLdcMatchType() {
        return ldcMatchType;
    }

    public void setMatchedLdcMatchKeyTuple(MatchKeyTuple matchedLdcMatchKeyTuple) {
        this.matchedLdcMatchKeyTuple = matchedLdcMatchKeyTuple;
    }

    public MatchKeyTuple getMatchedLdcMatchKeyTuple() {
        return matchedLdcMatchKeyTuple;
    }

    public void setExistingLookupKeyList(List<Pair<String, MatchKeyTuple>> existingLookupKeyList) {
        this.existingLookupKeyList = existingLookupKeyList;
    }

    public List<Pair<String, MatchKeyTuple>> getExistingLookupKeyList() {
        return existingLookupKeyList;
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

    public void setL2aEntityMatchType(EntityMatchType l2aEntityMatchType) {
        this.l2aEntityMatchType = l2aEntityMatchType.name();
    }

    public String getL2aEntityMatchType() {
        return l2aEntityMatchType;
    }

    public void setL2aMatchedEntityMatchKeyTuple(MatchKeyTuple l2aMatchedEntityMatchKeyTuple) {
        this.l2aMatchedEntityMatchKeyTuple = l2aMatchedEntityMatchKeyTuple;
    }

    public MatchKeyTuple getL2aMatchedEntityMatchKeyTuple() {
        return l2aMatchedEntityMatchKeyTuple;
    }

    public void setL2aLdcMatchType(LdcMatchType l2aLdcMatchType) {
        this.l2aLdcMatchType = l2aLdcMatchType.name();
    }

    public String getL2aLdcMatchType() {
        return l2aLdcMatchType;
    }

    public void setL2aMatchedLdcMatchKeyTuple(MatchKeyTuple l2aMatchedLdcMatchKeyTuple) {
        this.l2aMatchedLdcMatchKeyTuple = l2aMatchedLdcMatchKeyTuple;
    }

    public MatchKeyTuple getL2aMatchedLdcMatchKeyTuple() {
        return l2aMatchedLdcMatchKeyTuple;
    }

    public void setL2aExistingLookupKeyList(List<Pair<String, MatchKeyTuple>> l2aExistingLookupKeyList) {
        this.l2aExistingLookupKeyList = l2aExistingLookupKeyList;
    }

    public List<Pair<String, MatchKeyTuple>> getL2aExistingLookupKeyList() {
        return l2aExistingLookupKeyList;
    }
}

