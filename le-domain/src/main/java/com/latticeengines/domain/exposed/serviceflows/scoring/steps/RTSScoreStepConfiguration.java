package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class RTSScoreStepConfiguration extends MicroserviceStepConfiguration {

    private Boolean registerScoredTable = false;

    private String modelId;

    private String inputTableName;

    private boolean enableLeadEnrichment;

    private boolean enableDebug;

    private boolean enableMatching = Boolean.TRUE;

    private boolean scoreTestFile;

    private String modelType;

    private String idColumnName = InterfaceName.InternalId.name();

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getInputTableName() {
        return this.inputTableName;
    }

    public void setInputTableName(String inputTableName) {
        this.inputTableName = inputTableName;
    }

    public boolean getEnableLeadEnrichment() {
        return this.enableLeadEnrichment;
    }

    public void setEnableLeadEnrichment(boolean enableLeadEnrichment) {
        this.enableLeadEnrichment = enableLeadEnrichment;
    }

    public Boolean isRegisterScoredTable() {
        return this.registerScoredTable;
    }

    public void setRegisterScoredTable(Boolean registerScoredTable) {
        this.registerScoredTable = registerScoredTable;
    }

    public boolean getEnableDebug() {
        return this.enableDebug;
    }

    public void setEnableDebug(boolean enableDebug) {
        this.enableDebug = enableDebug;
    }

    public boolean getEnableMatching() {
        return this.enableMatching;
    }

    public void setEnableMatching(boolean enableMatching) {
        this.enableMatching = enableMatching;
    }

    public String getModelType() {
        return this.modelType;
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    public boolean getScoreTestFile() {
        return this.scoreTestFile;
    }

    public void setScoreTestFile(boolean scoreTestFile) {
        this.scoreTestFile = scoreTestFile;
    }

    public String getIdColumnName() {
        return idColumnName;
    }

    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }
}
