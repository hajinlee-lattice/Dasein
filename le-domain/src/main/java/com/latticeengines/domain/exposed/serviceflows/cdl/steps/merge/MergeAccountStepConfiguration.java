package com.latticeengines.domain.exposed.serviceflows.cdl.steps.merge;

import com.latticeengines.domain.exposed.query.BusinessEntity;

public class MergeAccountStepConfiguration extends BaseMergeImportStepConfiguration {

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.Account;
    }

}
