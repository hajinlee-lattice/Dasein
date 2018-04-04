package com.latticeengines.domain.exposed.serviceflows.scoring.steps;

public class PivotScoreAndEventConfiguration extends BaseScoringDataFlowStepConfiguration {

    private String eventColumn;
    private boolean expectedValue;
    private boolean liftChart;
    private String ratingEngineId;
    private String modelId;

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

    public boolean isLiftChart() {
        return liftChart;
    }

    public void setLiftChart(boolean liftChart) {
        this.liftChart = liftChart;
    }

    public void setRatingEngineId(String ratingEngineId) {
        this.ratingEngineId = ratingEngineId;
    }

    public String getRatingEngineId() {
        return this.ratingEngineId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getModelId() {
        return this.modelId;
    }

}
