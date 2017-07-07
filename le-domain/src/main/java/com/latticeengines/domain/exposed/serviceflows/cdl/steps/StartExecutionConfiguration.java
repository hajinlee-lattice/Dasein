package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartExecutionConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_status")
    private Status datafeedStatus;

    public Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

}
