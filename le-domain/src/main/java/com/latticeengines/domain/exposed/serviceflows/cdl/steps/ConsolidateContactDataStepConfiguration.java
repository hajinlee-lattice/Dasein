package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateContactDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    public ConsolidateContactDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Contact;
    }
}
