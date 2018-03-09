package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessContactStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeContactWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeContactWrapper extends BaseTransformationWrapper<ProcessContactStepConfiguration, MergeContact> {

    @Inject
    private MergeContact mergeContact;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeContact.BEAN_NAME);
    }

    @Override
    protected MergeContact getWrapperStep() {
        return mergeContact;
    }

}
