package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;

public class PrepareMatchDataConfiguration extends BaseCoreDataFlowStepConfiguration {

    @JsonProperty("input_table_name")
    private String inputTableName;
    @JsonProperty("id_column_name")
    private String idColumnName = InterfaceName.Id.name();

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

}
