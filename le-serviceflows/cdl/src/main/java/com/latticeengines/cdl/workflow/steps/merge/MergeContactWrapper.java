package com.latticeengines.cdl.workflow.steps.merge;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("mergeContactWrapper")
public class MergeContactWrapper extends BaseTransformationWrapper<MergeContact> {

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
