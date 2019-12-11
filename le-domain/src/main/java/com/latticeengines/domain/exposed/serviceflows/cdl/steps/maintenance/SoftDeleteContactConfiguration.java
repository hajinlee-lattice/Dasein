package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SoftDeleteContactConfiguration extends BaseSoftDeleteEntityConfiguration {
    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Contact;
    }
}
