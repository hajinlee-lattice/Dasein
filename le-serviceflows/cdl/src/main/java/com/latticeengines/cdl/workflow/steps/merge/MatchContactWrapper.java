package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("MatchContactWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchContactWrapper extends BaseTransformationWrapper<ProcessContactStepConfiguration, MatchContact> {

    @Inject
    private MatchContact matchContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MatchContact.BEAN_NAME);
    }

    @Override
    protected MatchContact getWrapperStep() {
        return matchContact;
    }

}
