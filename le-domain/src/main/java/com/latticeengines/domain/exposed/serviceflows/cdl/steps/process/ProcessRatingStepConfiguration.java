package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessRatingStepConfiguration extends BaseProcessEntityStepConfiguration {

    @JsonProperty("max_iteration")
    private int maxIteration;

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Rating;
    }

    public int getMaxIteration() {
        return maxIteration;
    }

    public void setMaxIteration(int maxIteration) {
        this.maxIteration = maxIteration;
    }
}
