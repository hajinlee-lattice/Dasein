package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

public class PivotScoreAndEventConfiguration extends BaseScoringDataFlowStepConfiguration {

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
