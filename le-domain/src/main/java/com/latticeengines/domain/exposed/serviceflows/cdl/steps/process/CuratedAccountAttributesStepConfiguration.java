package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class CuratedAccountAttributesStepConfiguration extends BaseProcessEntityStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.CuratedAccount;
    }

    @Override
    public Long getDataQuotaLimit() {
        return null;
    }

    @Override
    public Long getDataQuotaLimit(ProductType type) {
        return null;
    }
}
