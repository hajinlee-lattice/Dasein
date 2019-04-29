package com.latticeengines.domain.exposed.datacloud.match;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EntityMatchHistory {
    @JsonProperty("BusinessEntity")
    private String businessEntity;

    @JsonProperty("EntityId")
    private String entityId;

    @JsonProperty("UserId")
    private String userId;

    @JsonProperty("MatchKeyTuple")
    private MatchKeyTuple matchKeyTuple;

    public void setBusinessEntity(String businessEntity) {
        this.businessEntity = businessEntity;
    }

    public String getBusinessEntity() {
        return businessEntity;
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

    public void setMatchKeyTuple(MatchKeyTuple matchKeyTuple) {
        this.matchKeyTuple = matchKeyTuple;
    }

    public MatchKeyTuple getMatchKeyTuple() {
        return matchKeyTuple;
    }
}
