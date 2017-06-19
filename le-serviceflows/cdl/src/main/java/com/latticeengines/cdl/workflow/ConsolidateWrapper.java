package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.ConsolidateData;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("consolidateWrapper")
public class ConsolidateWrapper extends BaseTransformationWrapper<ConsolidateData> {

    @Autowired
    private ConsolidateData consolidateData;

    @Override
    protected ConsolidateData getWrapperStep() {
        return consolidateData;
    }

}