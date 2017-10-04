package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.SortProductStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("sortProduct")
public class SortProductWrapper extends BaseTransformationWrapper<SortProductStep> {

    @Autowired
    private SortProductStep sortProductStep;

    @PostConstruct
    public void postConstruct() {
        // override transformation step bean name
        setTransformationStepBeanName("sortProductTransformStep");
    }

    @Override
    protected SortProductStep getWrapperStep() {
        return sortProductStep;
    }

}
