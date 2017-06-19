package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.cdl.workflow.steps.CalculateStatsStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;
import org.springframework.stereotype.Component;

@Component("calculateStatsWrapper")
public class CalculateStatsWrapper extends BaseTransformationWrapper<CalculateStatsStep> {

    @Autowired
    private CalculateStatsStep calculateStatsStep;

    @Override
    protected CalculateStatsStep getWrapperStep() {
        return calculateStatsStep;
    }

}
