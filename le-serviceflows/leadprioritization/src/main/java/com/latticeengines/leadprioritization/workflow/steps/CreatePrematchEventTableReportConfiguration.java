package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.report.BaseDataFlowReportStepConfiguration;

public class CreatePrematchEventTableReportConfiguration extends BaseDataFlowReportStepConfiguration {
    @NotNull
    @NotEmptyString
    private String sourceTableName;

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }
}
