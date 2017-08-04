package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CalculateStatsStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("calculateStatsWrapper")
public class CalculateStatsWrapper extends BaseTransformationWrapper<CalculateStatsStep> {

    @Autowired
    private CalculateStatsStep calculateStatsStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("calculateStatsTransformStep");
    }

    @Override
    protected CalculateStatsStep getWrapperStep() {
        return calculateStatsStep;
    }

}
