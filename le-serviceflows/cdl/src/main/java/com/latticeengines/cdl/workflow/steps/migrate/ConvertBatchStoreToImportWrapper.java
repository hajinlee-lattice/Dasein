package com.latticeengines.cdl.workflow.steps.migrate;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformationWrapper;

@Component("convertBatchStoreToImportWrapper")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToImportWrapper
        extends BaseTransformationWrapper<ConvertBatchStoreStepConfiguration, ConvertBatchStoreToImport> {

    @Inject
    private ConvertBatchStoreToImport convertBatchStoreToImport;

    @PostConstruct
    public void overrideTransformationStepBeanName() {
        setTransformationStepBeanName(ConvertBatchStoreToImport.BEAN_NAME);
    }

    @Override
    protected ConvertBatchStoreToImport getWrapperStep() {
        return convertBatchStoreToImport;
    }
}
