package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ExportStepConfiguration;

public class ExportScoreTrainingFileStepConfiguration extends ExportStepConfiguration {

    @JsonProperty("exportInclusionColumns")
    private String exportInclusionColumns;

    public void setExportInclusionColumns(String exportInclusionColumns) {
        this.exportInclusionColumns = exportInclusionColumns;
    }

    public String getExportInclusionColumns() {
        return this.exportInclusionColumns;
    }
}
