package com.latticeengines.cdl.workflow.steps.rebuild;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessAccountStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("generateBucketedAccountWrapper")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class GenerateBucketedAccountWrapper extends BaseTransformationWrapper<ProcessAccountStepConfiguration, GenerateBucketedAccount> {

    @Inject
    private GenerateBucketedAccount generateBucketedAccount;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(GenerateBucketedAccount.BEAN_NAME);
    }

    @Override
    protected GenerateBucketedAccount getWrapperStep() {
        return generateBucketedAccount;
    }

}
