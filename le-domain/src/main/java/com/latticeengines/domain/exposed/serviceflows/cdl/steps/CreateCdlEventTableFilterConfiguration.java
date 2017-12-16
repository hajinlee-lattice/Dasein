package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class CreateCdlEventTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String trainFilterTableName;
    private String eventFilterTableName;

    private FrontEndQuery trainQuery;
    private FrontEndQuery eventQuery;

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

    public void setTrainQuery(FrontEndQuery trainQuery) {
        this.trainQuery = trainQuery;
    }

    public FrontEndQuery getTrainQuery() {
        return trainQuery;
    }

    public void setEventQuery(FrontEndQuery eventQuery) {
        this.eventQuery = eventQuery;
    }

    public FrontEndQuery getEventQuery() {
        return eventQuery;
    }

}
