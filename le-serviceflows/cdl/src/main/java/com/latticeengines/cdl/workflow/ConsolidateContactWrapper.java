package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ConsolidateContactData;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateContactWrapper")
public class ConsolidateContactWrapper extends BaseTransformationWrapper<ConsolidateContactData> {

    @Autowired
    private ConsolidateContactData consolidateContactData;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("consolidateContactDataTransformStep");
    }

    @Override
    protected ConsolidateContactData getWrapperStep() {
        return consolidateContactData;
    }
}
