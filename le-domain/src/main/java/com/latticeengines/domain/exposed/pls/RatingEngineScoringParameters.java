package com.latticeengines.domain.exposed.pls;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class RatingEngineScoringParameters {

    @JsonProperty
    private EventFrontEndQuery targetFilterQuery;

    @JsonProperty
    private String tableToScoreName;

    @JsonProperty
    String sourceDisplayName;

    @JsonProperty
    private boolean expectedValue;

    @JsonProperty
    private boolean liftChart;

    public EventFrontEndQuery getTargetFilterQuery() {
        return targetFilterQuery;
    }

    public void setTargetFilterQuery(EventFrontEndQuery targetFilterQuery) {
        this.targetFilterQuery = targetFilterQuery;
    }

    public String getTableToScoreName() {
        return tableToScoreName;
    }

    public void setTableToScoreName(String tableToScoreName) {
        this.tableToScoreName = tableToScoreName;
    }

    public String getSourceDisplayName() {
        return sourceDisplayName;
    }

    public void setSourceDisplayName(String sourceDisplayName) {
        this.sourceDisplayName = sourceDisplayName;
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
}
