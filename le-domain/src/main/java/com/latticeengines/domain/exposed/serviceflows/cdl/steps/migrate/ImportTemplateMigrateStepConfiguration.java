package com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportTemplateMigrateStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("datafeedtask_list")
    private List<String> dataFeedTaskList;

    public List<String> getDataFeedTaskList() {
        return dataFeedTaskList;
    }

    public void setDataFeedTaskList(List<String> dataFeedTaskList) {
        this.dataFeedTaskList = dataFeedTaskList;
    }
}
