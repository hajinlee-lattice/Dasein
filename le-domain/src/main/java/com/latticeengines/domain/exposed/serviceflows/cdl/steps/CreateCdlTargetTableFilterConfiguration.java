package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class CreateCdlTargetTableFilterConfiguration extends BaseCDLDataFlowStepConfiguration {

    private String targetFilterTableName;

    private EventFrontEndQuery targetQuery;

    private DataCollection.Version version;

    public CreateCdlTargetTableFilterConfiguration() {
        setBeanName("createCdlTargetTableFilterFlow");
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public EventFrontEndQuery getTargetQuery() {
        return targetQuery;
    }

    public void setTargetQuery(EventFrontEndQuery targetQuery) {
        this.targetQuery = targetQuery;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }
}
