package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportDataFeedTaskConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_task_id")
    private String dataFeedTaskId;

    @JsonProperty("import_config")
    private String importConfig;

    public String getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(String dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }

    public String getImportConfig() {
        return importConfig;
    }

    public void setImportConfig(String importConfig) {
        this.importConfig = importConfig;
    }
}
