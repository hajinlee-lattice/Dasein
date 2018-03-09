package com.latticeengines.cdl.workflow.steps.update;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("processAccountDiffWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ProcessAccountDiffWrapper
        extends BaseTransformationWrapper<ProcessAccountStepConfiguration, ProcessAccountDiff> {

    @Inject
    private ProcessAccountDiff processAccountDiff;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ProcessAccountDiff.BEAN_NAME);
    }

    @Override
    protected ProcessAccountDiff getWrapperStep() {
        return processAccountDiff;
    }

}
