package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessProductStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeProductImportsWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeProductImportsWrapper extends BaseTransformationWrapper<ProcessProductStepConfiguration, MergeProductImports> {

    @Inject
    private MergeProductImports mergeProductImports;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeProductImports.BEAN_NAME);
    }

    @Override
    protected MergeProductImports getWrapperStep() {
        return mergeProductImports;
    }

}
