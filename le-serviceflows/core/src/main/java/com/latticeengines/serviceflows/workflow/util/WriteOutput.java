package com.latticeengines.serviceflows.workflow.util;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("writeOutput")
public class WriteOutput extends BaseWorkflowStep<WriteOutputStepConfiguration> {
    @Override
    public void execute() {
        for (String key : configuration.getOutput().keySet()) {
            putOutputValue(key, configuration.getOutput().get(key));
        }
    }
}
