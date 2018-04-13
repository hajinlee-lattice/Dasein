package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

public class PivotScoreAndEventConfiguration extends BaseScoringDataFlowStepConfiguration {

    private String eventColumn;
    private boolean expectedValue;
    private boolean deferSavingBucketedScoreSummaries;

    public PivotScoreAndEventConfiguration() {
        setBeanName("pivotScoreAndEvent");
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public boolean isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

    public boolean isDeferSavingBucketedScoreSummaries() {
        return deferSavingBucketedScoreSummaries;
    }

    public void setDeferSavingBucketedScoreSummaries(boolean deferSavingBucketedScoreSummaries) {
        this.deferSavingBucketedScoreSummaries = deferSavingBucketedScoreSummaries;
    }
}
