package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CreateCdlEventTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String trainFilterTableName;
    private String eventFilterTableName;

    private EventFrontEndQuery trainQuery;
    private EventFrontEndQuery eventQuery;

    public CreateCdlEventTableFilterConfiguration() {
        setBeanName("createCdlEventTableFilterFlow");
    }

    public void setTrainFilterTableName(String trainFilterTableName) {
        this.trainFilterTableName = trainFilterTableName;
    }

    public void setEventFilterTableName(String eventFilterTableName) {
        this.eventFilterTableName = eventFilterTableName;
    }

    public String getTrainFilterTableName() {
        return trainFilterTableName;
    }

    public String getEventFilterTableName() {
        return eventFilterTableName;
    }

    public void setTrainQuery(EventFrontEndQuery trainQuery) {
        this.trainQuery = trainQuery;
    }

    public EventFrontEndQuery getTrainQuery() {
        return trainQuery;
    }

    public void setEventQuery(EventFrontEndQuery eventQuery) {
        this.eventQuery = eventQuery;
    }

    public EventFrontEndQuery getEventQuery() {
        return eventQuery;
    }

}
