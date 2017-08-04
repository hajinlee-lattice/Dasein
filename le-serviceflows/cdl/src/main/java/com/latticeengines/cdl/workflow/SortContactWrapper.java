package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.SortContactStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("sortContact")
public class SortContactWrapper extends BaseTransformationWrapper<SortContactStep> {

    @Autowired
    private SortContactStep sortContactStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("sortContactTransformStep");
    }

    @Override
    protected SortContactStep getWrapperStep() {
        return sortContactStep;
    }

}
