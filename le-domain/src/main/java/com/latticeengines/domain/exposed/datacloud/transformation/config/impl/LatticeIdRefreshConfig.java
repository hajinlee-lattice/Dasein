package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LatticeIdRefreshConfig extends TransformerConfig {
    @JsonProperty("Strategy")
    private String strategy;

    @JsonProperty("CurrentCount")
    private Long currentCount;

    @JsonProperty("IdSrcIdx")
    private Integer idSrcIdx;

    @JsonProperty("EntitySrcIdx")
    private Integer entitySrcIdx;

    public String getStrategy() {
        return strategy;
    }

    public void setStrategy(String strategy) {
        this.strategy = strategy;
    }

    public Long getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(Long currentCount) {
        this.currentCount = currentCount;
    }

    public Integer getIdSrcIdx() {
        return idSrcIdx;
    }

    public void setIdSrcIdx(Integer idSrcIdx) {
        this.idSrcIdx = idSrcIdx;
    }

    public Integer getEntitySrcIdx() {
        return entitySrcIdx;
    }

    public void setEntitySrcIdx(Integer entitySrcIdx) {
        this.entitySrcIdx = entitySrcIdx;
    }

}
