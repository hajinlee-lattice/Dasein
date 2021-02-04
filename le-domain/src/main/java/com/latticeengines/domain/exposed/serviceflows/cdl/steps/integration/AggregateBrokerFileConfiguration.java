package com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class AggregateBrokerFileConfiguration extends BaseStepConfiguration {

    private CustomerSpace customerSpace;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }
}
