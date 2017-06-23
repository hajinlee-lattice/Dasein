package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.metadata.DataFeed.Status;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartExecutionConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_name")
    @NotEmptyString
    @NotNull
    private String datafeedName;

    @JsonProperty("data_feed_status")
    private Status datafeedStatus;

    public String getDataFeedName() {
        return datafeedName;
    }

    public void setDataFeedName(String datafeedName) {
        this.datafeedName = datafeedName;
    }

    public Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

}
