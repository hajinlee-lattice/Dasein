package com.latticeengines.cdl.workflow.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartExecutionConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_name")
    @NotEmptyString
    @NotNull
    private String datafeedName;

    public String getDataFeedName() {
        return datafeedName;
    }

    public void setDataFeedName(String datafeedName) {
        this.datafeedName = datafeedName;
    }

}
