package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.CleanupByUploadStep;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("cleanupByUploadWrapper")
public class CleanupByUploadWrapper extends BaseTransformationWrapper<CleanupByUploadStep> {

    @Autowired
    private CleanupByUploadStep cleanupByUploadStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName("cleanupByUploadStep");
    }

    @Override
    protected CleanupByUploadStep getWrapperStep() {
        return cleanupByUploadStep;
    }
}
