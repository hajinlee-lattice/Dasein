package com.latticeengines.cdl.workflow.steps.legacydelete;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteByUploadStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("legacyDeleteByUploadStepWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteByUploadStepWrapper extends BaseTransformationWrapper<LegacyDeleteByUploadStepConfiguration,
        LegacyDeleteByUploadStep> {

    @Inject
    private LegacyDeleteByUploadStep legacyDeleteByUploadStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(LegacyDeleteByUploadStep.BEAN_NAME);
    }

    @Override
    protected LegacyDeleteByUploadStep getWrapperStep() {
        return legacyDeleteByUploadStep;
    }
}
