package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeAccountWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, MergeAccount> {

    @Inject
    private MergeAccount mergeAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeAccount.BEAN_NAME);
    }

    @Override
    protected MergeAccount getWrapperStep() {
        return mergeAccount;
    }

}
