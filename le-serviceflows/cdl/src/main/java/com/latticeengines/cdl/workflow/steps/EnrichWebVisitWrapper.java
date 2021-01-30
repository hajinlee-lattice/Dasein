package com.latticeengines.cdl.workflow.steps;


import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.EnrichWebVisitStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("EnrichWebVisitWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class EnrichWebVisitWrapper extends BaseTransformationWrapper<EnrichWebVisitStepConfiguration, EnrichWebVisit> {

    @Inject
    private EnrichWebVisit enrichWebVisit;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(EnrichWebVisit.BEAN_NAME);
    }

    @Override
    protected EnrichWebVisit getWrapperStep() {
        return enrichWebVisit;
    }
}
