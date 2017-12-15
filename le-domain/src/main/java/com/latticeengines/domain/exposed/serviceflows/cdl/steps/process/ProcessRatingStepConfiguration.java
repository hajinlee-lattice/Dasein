package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.BusinessEntity;

import java.util.List;

public class ProcessRatingStepConfiguration extends BaseProcessEntityStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Rating;
    }

    @JsonProperty("models")
    private List<RatingModel> models;

    public List<RatingModel> getModels() {
        return models;
    }

    public void setModels(List<RatingModel> models) {
        this.models = models;
    }

}
