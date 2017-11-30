package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed.Status;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class StartProcessingConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_status")
    private Status datafeedStatus;

    @JsonProperty("import_job_ids")
    private List<Long> importJobIds = Collections.emptyList();

    public Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

    public List<Long> getImportJobIds() {
        return importJobIds;
    }

    public void setImportJobIds(List<Long> importJobIds) {
        this.importJobIds = importJobIds;
    }
}
