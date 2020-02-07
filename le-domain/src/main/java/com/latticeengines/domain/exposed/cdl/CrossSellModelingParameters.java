package com.latticeengines.domain.exposed.cdl;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CrossSellModelingParameters extends ModelingParameters {

    @JsonProperty
    private EventFrontEndQuery trainFilterQuery;

    @JsonProperty
    private EventFrontEndQuery eventFilterQuery;

    @JsonProperty
    private EventFrontEndQuery targetFilterQuery;

    @JsonProperty
    private String tableName;

    @JsonProperty
    private boolean expectedValue;

    @JsonProperty
    private boolean liftChart = true;

    @JsonProperty
    private Integer modelIteration;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public EventFrontEndQuery getTrainFilterQuery() {
        return trainFilterQuery;
    }

    public void setTrainFilterQuery(EventFrontEndQuery trainFilterQuery) {
        this.trainFilterQuery = trainFilterQuery;
    }

    public EventFrontEndQuery getEventFilterQuery() {
        return eventFilterQuery;
    }

    public void setEventFilterQuery(EventFrontEndQuery eventFilterQuery) {
        this.eventFilterQuery = eventFilterQuery;
    }

    public EventFrontEndQuery getTargetFilterQuery() {
        return targetFilterQuery;
    }

    public void setTargetFilterQuery(EventFrontEndQuery targetFilterQuery) {
        this.targetFilterQuery = targetFilterQuery;
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

    public Integer getModelIteration() {
        return modelIteration;
    }

    public void setModelIteration(Integer modelIteration) {
        this.modelIteration = modelIteration;
    }
}
