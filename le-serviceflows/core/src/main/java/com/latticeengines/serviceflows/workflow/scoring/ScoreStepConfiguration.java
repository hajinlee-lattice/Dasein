package com.latticeengines.serviceflows.workflow.scoring;

import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class ScoreStepConfiguration extends MicroserviceStepConfiguration {

    private String modelId;
    
    private String sourceDir;
    
    private String uniqueKeyColumn;
    
    private Boolean registerScoredTable = false;
    
    public String getModelId() {
        return modelId;
    }
    
    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getSourceDir() {
        return sourceDir;
    }

    public void setSourceDir(String sourceDir) {
        this.sourceDir = sourceDir;
    }

    public String getUniqueKeyColumn() {
        return uniqueKeyColumn;
    }
    
    public void setUniqueKeyColumn(String uniqueKeyColumn) {
        this.uniqueKeyColumn = uniqueKeyColumn;
    }

    public Boolean isRegisterScoredTable() {
        return registerScoredTable;
    }

    public void setRegisterScoredTable(Boolean registerScoredTable) {
        this.registerScoredTable = registerScoredTable;
    }

}
