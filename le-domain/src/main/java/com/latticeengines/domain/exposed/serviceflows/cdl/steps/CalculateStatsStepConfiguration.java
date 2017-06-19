package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class CalculateStatsStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("data_collection_name")
    @NotNull
    private String dataCollectionName;

    public String getDataCollectionName() {
        return dataCollectionName;
    }

    public void setDataCollectionName(String dataCollectionName) {
        this.dataCollectionName = dataCollectionName;
    }

}
