package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;


public class GenerateRatingStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("models")
    private List<RatingModel> models;

    @JsonProperty("target_table")
    private String targetTable;

    public List<RatingModel> getModels() {
        return models;
    }

    public void setModels(List<RatingModel> models) {
        this.models = models;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public void setTargetTable(String targetTable) {
        this.targetTable = targetTable;
    }

}
