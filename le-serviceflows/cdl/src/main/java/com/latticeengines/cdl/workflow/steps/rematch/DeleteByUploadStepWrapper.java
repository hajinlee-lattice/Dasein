package com.latticeengines.cdl.workflow.steps.rematch;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.rematch.DeleteByUploadStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("deleteByUploadStepWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class DeleteByUploadStepWrapper
        extends BaseTransformationWrapper<DeleteByUploadStepConfiguration, DeleteByUploadStep> {

    @Inject
    private DeleteByUploadStep deleteByUploadStep;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(DeleteByUploadStep.BEAN_NAME);
    }

    @Override
    protected DeleteByUploadStep getWrapperStep() {
        return deleteByUploadStep;
    }
}
