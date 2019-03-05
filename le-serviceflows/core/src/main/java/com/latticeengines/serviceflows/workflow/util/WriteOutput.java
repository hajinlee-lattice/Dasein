package com.latticeengines.serviceflows.workflow.util;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.core.steps.WriteOutputStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("writeOutput")
public class WriteOutput extends BaseWorkflowStep<WriteOutputStepConfiguration> {
    @Override
    public void execute() {
        for (String key : configuration.getOutput().keySet()) {
            saveOutputValue(key, configuration.getOutput().get(key));
        }
    }
}
