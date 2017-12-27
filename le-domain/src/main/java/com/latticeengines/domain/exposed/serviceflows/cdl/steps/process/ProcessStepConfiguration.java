package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ProcessStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_status")
    private DataFeed.Status datafeedStatus;

    @JsonProperty("import_job_ids")
    private List<Long> importJobIds = Collections.emptyList();

    @JsonProperty("action_ids")
    private List<Long> actionIds = Collections.emptyList();

    public DataFeed.Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

    public List<Long> getImportJobIds() {
        return importJobIds;
    }

    public void setImportJobIds(List<Long> importJobIds) {
        this.importJobIds = importJobIds;
    }

    public List<Long> getActionIds() {
        return this.actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
    }
}
