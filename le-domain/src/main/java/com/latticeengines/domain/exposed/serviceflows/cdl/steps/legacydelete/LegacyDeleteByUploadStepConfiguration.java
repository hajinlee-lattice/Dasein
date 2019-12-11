package com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class LegacyDeleteByUploadStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("entity")
    private BusinessEntity entity;

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }
}
