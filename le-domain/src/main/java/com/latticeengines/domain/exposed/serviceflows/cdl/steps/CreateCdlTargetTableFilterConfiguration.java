package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class CreateCdlTargetTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String targetFilterTableName;

    private FrontEndQuery targetQuery;

    public CreateCdlTargetTableFilterConfiguration() {
        setBeanName("createCdlTargetTableFilterFlow");
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTargetQuery(FrontEndQuery targetQuery) {
        this.targetQuery = targetQuery;
    }

    public FrontEndQuery getTargetQuery() {
        return targetQuery;
    }
}
