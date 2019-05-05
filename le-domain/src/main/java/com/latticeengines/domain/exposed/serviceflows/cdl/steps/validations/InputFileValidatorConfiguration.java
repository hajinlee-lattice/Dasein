package com.latticeengines.domain.exposed.serviceflows.cdl.steps.validations;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class InputFileValidatorConfiguration extends BaseInputFileValidatorConfiguration {
    private BusinessEntity entity;

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

}
