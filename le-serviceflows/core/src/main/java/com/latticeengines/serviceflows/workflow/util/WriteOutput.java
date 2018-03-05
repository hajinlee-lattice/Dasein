package com.latticeengines.serviceflows.workflow.util;

import com.latticeengines.domain.exposed.serviceflows.core.steps.WriteOutputStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

import org.springframework.stereotype.Component;

@Component("writeOutput")
public class WriteOutput extends BaseWorkflowStep<WriteOutputStepConfiguration> {
    @Override
    public void execute() {
        for (String key : configuration.getOutput().keySet()) {
            saveOutputValue(key, configuration.getOutput().get(key));
        }
    }
}
