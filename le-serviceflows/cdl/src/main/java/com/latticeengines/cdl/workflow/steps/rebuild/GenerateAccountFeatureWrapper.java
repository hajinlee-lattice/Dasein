package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("generateAccountFeatureWrapper")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateAccountFeatureWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, GenerateAccountFeature> {

    @Inject
    private GenerateAccountFeature generateAccountFeature;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(GenerateAccountFeature.BEAN_NAME);
    }

    @Override
    protected GenerateAccountFeature getWrapperStep() {
        return generateAccountFeature;
    }

}
