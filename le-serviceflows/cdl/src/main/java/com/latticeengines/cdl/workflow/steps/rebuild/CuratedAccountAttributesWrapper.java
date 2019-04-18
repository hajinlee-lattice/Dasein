package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.CuratedAccountAttributesStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("curatedAccountAttributesWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CuratedAccountAttributesWrapper
        extends BaseTransformationWrapper<CuratedAccountAttributesStepConfiguration, CuratedAccountAttributesStep> {

    @Inject
    private CuratedAccountAttributesStep curatedAccountAttributesStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(CuratedAccountAttributesStep.BEAN_NAME);
    }

    @Override
    protected CuratedAccountAttributesStep getWrapperStep() {
        return curatedAccountAttributesStep;
    }

}
