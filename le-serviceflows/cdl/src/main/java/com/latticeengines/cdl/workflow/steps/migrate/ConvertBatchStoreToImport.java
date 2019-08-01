package com.latticeengines.cdl.workflow.steps.migrate;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.ConvertBatchStoreStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(ConvertBatchStoreToImport.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class ConvertBatchStoreToImport extends BaseTransformWrapperStep<ConvertBatchStoreStepConfiguration> {

    static final String BEAN_NAME = "convertBatchStoreToImport";

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        return null;
    }

    @Override
    protected void onPostTransformationCompleted() {

    }
}
