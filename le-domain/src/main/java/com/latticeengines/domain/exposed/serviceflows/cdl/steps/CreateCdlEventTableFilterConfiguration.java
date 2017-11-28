package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class CreateCdlEventTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String trainFilterTableName;
    private String targetFilterTableName;

    private FrontEndQuery trainQuery;
    private FrontEndQuery targetQuery;

    public CreateCdlEventTableFilterConfiguration() {
        setBeanName("createCdlEventTableFilterFlow");
        setTargetTableName("CreateCdlEventTableFilter_" + System.currentTimeMillis());
    }

    public void setTrainFilterTableName(String trainFilterTableName) {
        this.trainFilterTableName = trainFilterTableName;
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public String getTrainFilterTableName() {
        return trainFilterTableName;
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTrainQuery(FrontEndQuery trainQuery) {
        this.trainQuery = trainQuery;
    }

    public FrontEndQuery getTrainQuery() {
        return trainQuery;
    }

    public void setTargetQuery(FrontEndQuery targetQuery) {
        this.targetQuery = targetQuery;
    }

    public FrontEndQuery getTargetQuery() {
        return targetQuery;
    }
}
