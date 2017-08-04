package com.latticeengines.serviceflows.workflow.etl;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;
import com.latticeengines.workflow.exposed.build.WorkflowInterface;

public interface TransformationWorkflow extends WorkflowInterface<TransformationWorkflowConfiguration> {

    BaseWorkflowStep<? extends BaseStepConfiguration> getTransformationStep();
}
