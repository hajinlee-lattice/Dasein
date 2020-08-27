package com.latticeengines.cdl.workflow.steps;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RollupProductStepWrapper
        extends BaseTransformationWrapper<ProcessTransactionStepConfiguration, RollupProductStep> {

    @Inject
    private RollupProductStep rollupProductStep;

    @Override
    protected RollupProductStep getWrapperStep() {
        return rollupProductStep;
    }
}
