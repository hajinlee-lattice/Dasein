package com.latticeengines.cdl.workflow.steps.reset;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.cdl.workflow.steps.EntityAwareWorkflowStep;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;

public abstract class BaseResetEntityStep<T extends BaseProcessEntityStepConfiguration>
        extends EntityAwareWorkflowStep<T> {

    @Override
    public void execute() {
        getResettingEntities().forEach(entity -> updateEntitySetInContext(RESET_ENTITIES, entity));
    }

    protected Collection<BusinessEntity> getResettingEntities() {
        return Collections.singleton(configuration.getMainEntity());
    }

}
