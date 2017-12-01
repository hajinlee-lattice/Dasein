package com.latticeengines.cdl.workflow.steps.process;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessStepConfiguration;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("finishProcessing")
public class FinishProcessing extends BaseWorkflowStep<ProcessStepConfiguration> {

    @Override
    public void execute() {
    }

}
