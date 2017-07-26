package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.LatticeIdStrategy;

public class LatticeIdRefreshFlowParameter extends TransformationFlowParameters {
    @JsonProperty("Strategy")
    private LatticeIdStrategy strategy;

    @JsonProperty("CurrentCount")
    private Long currentCount;

    @JsonProperty("IdSrcIdx")
    private Integer idSrcIdx;

    @JsonProperty("EntitySrcIdx")
    private Integer entitySrcIdx;

    public LatticeIdStrategy getStrategy() {
        return strategy;
    }

    public void setStrategy(LatticeIdStrategy strategy) {
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
