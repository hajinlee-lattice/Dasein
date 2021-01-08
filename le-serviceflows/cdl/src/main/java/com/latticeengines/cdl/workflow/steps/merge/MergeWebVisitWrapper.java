package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeWebVisitWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MergeWebVisitWrapper extends BaseTransformationWrapper<ProcessActivityStreamStepConfiguration,
        MergeWebVisit> {

    @Inject
    private MergeWebVisit mergeWebVisit;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(MergeWebVisit.BEAN_NAME);
    }

    @Override
    protected MergeWebVisit getWrapperStep() {
        return mergeWebVisit;
    }
}
