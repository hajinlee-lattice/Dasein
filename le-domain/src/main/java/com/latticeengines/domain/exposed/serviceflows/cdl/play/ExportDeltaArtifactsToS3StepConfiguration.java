package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class ExportDeltaArtifactsToS3StepConfiguration extends ImportExportS3StepConfiguration {

    private String executionId;

    public String getExecutionId() { return executionId; }

    public void setExecutionId(String executionId) { this.executionId = executionId; }
}
