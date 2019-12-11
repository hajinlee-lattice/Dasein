package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class SoftDeleteAccountConfiguration extends BaseSoftDeleteEntityConfiguration {
    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }
}
