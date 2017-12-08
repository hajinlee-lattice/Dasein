package com.latticeengines.cdl.workflow.steps.update;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessTransactionStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

@Component(ProcessTransactionDiff.BEAN_NAME)
public class ProcessTransactionDiff extends BaseTransformWrapperStep<ProcessTransactionStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(ProcessTransactionDiff.class);

    static final String BEAN_NAME = "processTransactionDiff";

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        throw new UnsupportedOperationException("Update mode is not supported for transaction, because it does not support version isolation.");
    }

    @Override
    protected void onPostTransformationCompleted() {
    }
}
