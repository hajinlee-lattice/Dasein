package com.latticeengines.domain.exposed.serviceflows.cdl.steps.update;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public abstract class BaseUpdateStepConfiguration extends BaseWrapperStepConfiguration {

    public abstract BusinessEntity getMainEntity();

}
