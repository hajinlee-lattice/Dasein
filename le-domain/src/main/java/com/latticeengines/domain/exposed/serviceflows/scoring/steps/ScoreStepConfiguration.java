package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ScoreStepConfiguration extends MicroserviceStepConfiguration {

    private String modelId;

    private String sourceDir;

    private String uniqueKeyColumn;

    private Boolean registerScoredTable = false;

    private Boolean useScorederivation = null;

    private Boolean readModelIdFromRecord = true;

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

    public Boolean getUseScorederivation() {
        return useScorederivation;
    }

    public void setUseScorederivation(Boolean useScorederivation) {
        this.useScorederivation = useScorederivation;
    }

    public Boolean getReadModelIdFromRecord() {
        return readModelIdFromRecord;
    }

    public void setModelIdFromRecord(Boolean readModelIdFromRecord) {
        this.readModelIdFromRecord = readModelIdFromRecord;
    }

}
