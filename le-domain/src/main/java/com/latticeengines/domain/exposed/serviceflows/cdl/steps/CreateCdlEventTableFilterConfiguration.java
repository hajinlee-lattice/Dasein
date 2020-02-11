package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CreateCdlEventTableFilterConfiguration extends GenerateRatingStepConfiguration {

    private EventFrontEndQuery trainQuery;
    private EventFrontEndQuery eventQuery;
    private boolean expectedValue;
    private String eventColumn;

    public CreateCdlEventTableFilterConfiguration() {
    }

    public EventFrontEndQuery getTrainQuery() {
        return trainQuery;
    }

    public void setTrainQuery(EventFrontEndQuery trainQuery) {
        this.trainQuery = trainQuery;
    }

    public EventFrontEndQuery getEventQuery() {
        return eventQuery;
    }

    public void setEventQuery(EventFrontEndQuery eventQuery) {
        this.eventQuery = eventQuery;
    }

    public boolean isExpectedValue() {
        return expectedValue;
    }

    public void setExpectedValue(boolean expectedValue) {
        this.expectedValue = expectedValue;
    }

    public String getEventColumn() {
        return eventColumn;
    }

    public void setEventColumn(String eventColumn) {
        this.eventColumn = eventColumn;
    }

}
