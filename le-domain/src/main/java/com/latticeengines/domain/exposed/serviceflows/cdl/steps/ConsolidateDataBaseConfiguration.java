package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class ConsolidateDataBaseConfiguration extends BaseWrapperStepConfiguration {

    public ConsolidateDataBaseConfiguration() {
        this.setSkipStep(true);
    }

    @JsonIgnore
    protected BusinessEntity businessEntity;

    public BusinessEntity getBusinessEntity() {
        return businessEntity;
    }
}
