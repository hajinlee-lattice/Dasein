package com.latticeengines.domain.exposed.serviceflows.cdl.steps.update;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class UpdateAccountStepConfiguration extends BaseUpdateStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

}
