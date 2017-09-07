package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateTransactionDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    public ConsolidateTransactionDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Transaction;
    }

}
