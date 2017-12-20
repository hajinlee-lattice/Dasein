package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CreateCdlTargetTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String targetFilterTableName;

    private EventFrontEndQuery targetQuery;

    public CreateCdlTargetTableFilterConfiguration() {
        setBeanName("createCdlTargetTableFilterFlow");
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTargetQuery(EventFrontEndQuery targetQuery) {
        this.targetQuery = targetQuery;
    }

    public EventFrontEndQuery getTargetQuery() {
        return targetQuery;
    }
}
