package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessTransactionStepConfiguration extends BaseProcessEntityStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Transaction;
    }

}
