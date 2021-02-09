package com.latticeengines.domain.exposed.serviceflows.cdl.steps.export;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

public class GenerateChangeTableConfiguration extends BaseProcessEntityStepConfiguration {
    @Override
    public BusinessEntity getMainEntity() {
        return null;
    }
}
