package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.swlib.SoftwareLibrary;

public class PivotScoreAndEventConfiguration extends BaseLPDataFlowStepConfiguration {

    private String eventColumn;

    public PivotScoreAndEventConfiguration() {
        setBeanName("pivotScoreAndEvent");
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    @Override
    public String getSwlib() {
        return SoftwareLibrary.Scoring.getName();
    }
}
