package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateContactWrapper")
public class ConsolidateContactWrapper extends BaseTransformationWrapper<ConsolidateContactData> {

    @Autowired
    private ConsolidateContactData consolidateContactData;

    @Override
    protected ConsolidateContactData getWrapperStep() {
        return consolidateContactData;
    }
}
