package com.latticeengines.cdl.workflow.steps.maintenance;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.RenameAndMatchStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("renameAndMatchStepWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RenameAndMatchStepWrapper
        extends BaseTransformationWrapper<RenameAndMatchStepConfiguration, RenameAndMatchStep> {

    @Inject
    private RenameAndMatchStep renameAndMatchStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(RenameAndMatchStep.BEAN_NAME);
    }

    @Override
    protected RenameAndMatchStep getWrapperStep() {
        return renameAndMatchStep;
    }
}
