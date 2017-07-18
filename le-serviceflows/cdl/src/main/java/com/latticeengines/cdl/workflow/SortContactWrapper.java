package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.cdl.workflow.steps.SortContactStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;
import org.springframework.stereotype.Component;

@Component("sortContact")
public class SortContactWrapper extends BaseTransformationWrapper<SortContactStep> {

    @Autowired
    private SortContactStep sortContactStep;

    @Override
    protected SortContactStep getWrapperStep() {
        return sortContactStep;
    }

}
