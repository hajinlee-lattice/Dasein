package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PrepareMatchDataConfiguration extends BaseCoreDataFlowStepConfiguration {

    @JsonProperty("input_table_name")
    private String inputTableName;

    @JsonProperty("match_group_id")
    private String matchGroupId;

    public PrepareMatchDataConfiguration() {
        setBeanName("prepareMatchDataflow");
    }

    public String getInputTableName() {
        return inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getMatchGroupId() {
        return matchGroupId;
    }

    public void setMatchGroupId(String matchGroupId) {
        this.matchGroupId = matchGroupId;
    }

}
