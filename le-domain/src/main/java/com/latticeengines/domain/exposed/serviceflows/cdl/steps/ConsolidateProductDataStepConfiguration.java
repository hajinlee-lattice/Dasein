package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ConsolidateProductDataStepConfiguration extends ConsolidateDataBaseConfiguration {

    public ConsolidateProductDataStepConfiguration() {
        super();
        this.businessEntity = BusinessEntity.Product;
    }

}
