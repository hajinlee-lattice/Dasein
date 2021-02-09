package com.latticeengines.cdl.workflow.steps.legacydelete;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.legacydelete.LegacyDeleteStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("LegacyDeleteByUploadWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class LegacyDeleteByUploadWrapper extends BaseTransformationWrapper<LegacyDeleteStepConfiguration, LegacyDeleteByUpload> {

    @Inject
    private LegacyDeleteByUpload legacyDeleteByUpload;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(LegacyDeleteByUpload.BEAN_NAME);
    }

    @Override
    protected LegacyDeleteByUpload getWrapperStep() {
        return legacyDeleteByUpload;
    }
}
