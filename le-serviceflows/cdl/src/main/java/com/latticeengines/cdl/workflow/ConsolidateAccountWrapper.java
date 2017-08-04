package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ConsolidateAccountData;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateAccountWrapper")
public class ConsolidateAccountWrapper extends BaseTransformationWrapper<ConsolidateAccountData> {

    @Autowired
    private ConsolidateAccountData consolidateAccountData;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("consolidateAccountDataTransformStep");
    }

    @Override
    protected ConsolidateAccountData getWrapperStep() {
        return consolidateAccountData;
    }

}