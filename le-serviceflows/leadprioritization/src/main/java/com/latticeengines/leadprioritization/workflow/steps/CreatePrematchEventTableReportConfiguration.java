package com.latticeengines.leadprioritization.workflow.steps;

import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.report.BaseDataFlowReportStepConfiguration;

public class CreatePrematchEventTableReportConfiguration extends BaseDataFlowReportStepConfiguration {
    @NotNull
    @NotEmptyString
    private String sourceTableName;

    @NotNull
    private Long minPositiveEvents;

    @NotNull
    private Long minDedupedRows;

    public String getSourceTableName() {
        return sourceTableName;
    }

    public void setSourceTableName(String sourceTableName) {
        this.sourceTableName = sourceTableName;
    }

    public Long getMinPositiveEvents() {
        return minPositiveEvents;
    }

    public void setMinPositiveEvents(Long minPositiveEvents) {
        this.minPositiveEvents = minPositiveEvents;
    }

    public Long getMinDedupedRows() {
        return minDedupedRows;
    }

    public void setMinDedupedRows(Long minDedupedRows) {
        this.minDedupedRows = minDedupedRows;
    }
}
