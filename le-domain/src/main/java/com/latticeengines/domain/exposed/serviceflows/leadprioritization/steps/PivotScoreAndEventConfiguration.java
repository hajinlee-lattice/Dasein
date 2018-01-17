package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

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
}
