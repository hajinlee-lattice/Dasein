package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class PrepareMatchDataConfiguration extends BaseCoreDataFlowStepConfiguration {

    @JsonProperty("input_table_name")
    private String inputTableName;
    @JsonProperty("id_column_name")
    private String idColumnName = InterfaceName.Id.name();
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

    public String getIdColumnName() {
        return idColumnName;
    }

    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    public String getMatchGroupId() {
        return matchGroupId;
    }

    public void setMatchGroupId(String matchGroupId) {
        this.matchGroupId = matchGroupId;
    }

}
