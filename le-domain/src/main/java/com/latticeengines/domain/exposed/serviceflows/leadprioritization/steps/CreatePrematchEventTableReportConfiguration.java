package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.serviceflows.core.steps.BaseDataFlowReportStepConfiguration;

public class CreatePrematchEventTableReportConfiguration extends BaseDataFlowReportStepConfiguration {
    @NotNull
    @NotEmptyString
    private String sourceTableName;

    @NotNull
    private Long minPositiveEvents;

    @NotNull
    private Long minNegativeEvents;

    @NotNull
    private Long minRows;

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

    public Long getMinNegativeEvents() {
        return minNegativeEvents;
    }

    public void setMinNegativeEvents(Long minNegativeEvents) {
        this.minNegativeEvents = minNegativeEvents;
    }

    public Long getMinRows() {
        return minRows;
    }

    public void setMinRows(Long minRows) {
        this.minRows = minRows;
    }
}
