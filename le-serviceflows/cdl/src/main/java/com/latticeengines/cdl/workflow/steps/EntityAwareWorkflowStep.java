package com.latticeengines.cdl.workflow.steps;

import java.util.HashSet;
import java.util.Set;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.BaseProcessEntityStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class EntityAwareWorkflowStep<T extends BaseProcessEntityStepConfiguration> extends BaseWorkflowStep<T> {

    protected void updateEntitySetInContext(String key, BusinessEntity entity) {
        Set<BusinessEntity> entitySet = getSetObjectFromContext(key, BusinessEntity.class);
        if (entitySet == null) {
            entitySet = new HashSet<>();
        }
        entitySet.add(entity);
        putObjectInContext(key, entitySet);
    }

}
