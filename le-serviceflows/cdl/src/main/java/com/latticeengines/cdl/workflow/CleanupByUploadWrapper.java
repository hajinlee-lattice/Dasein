package com.latticeengines.cdl.workflow;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.maintenance.CleanupByUploadStep;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance.CleanupByUploadWrapperConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("cleanupByUploadWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CleanupByUploadWrapper
        extends BaseTransformationWrapper<CleanupByUploadWrapperConfiguration, CleanupByUploadStep> {

    @Inject
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
