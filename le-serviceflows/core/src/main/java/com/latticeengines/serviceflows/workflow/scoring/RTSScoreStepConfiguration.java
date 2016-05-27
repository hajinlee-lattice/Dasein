package com.latticeengines.serviceflows.workflow.scoring;

import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class RTSScoreStepConfiguration extends MicroserviceStepConfiguration {

    private Boolean registerScoredTable = false;

    private String modelId;

    private String inputTableName;

    public String getModelId() {
        return modelId;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public String getInputTableName() {
        return this.inputTableName;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public Boolean isRegisterScoredTable() {
        return this.registerScoredTable;
    }

    public void setRegisterScoredTable(Boolean registerScoredTable) {
        this.registerScoredTable = registerScoredTable;
    }

}
