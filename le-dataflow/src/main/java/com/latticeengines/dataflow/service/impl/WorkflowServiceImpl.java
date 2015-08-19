package com.latticeengines.dataflow.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.service.WorkflowService;

@Component("orchestrationService")
public class WorkflowServiceImpl implements WorkflowService {

    @Override
    public void executeNamedOrchestrationFlow(String orchestrationFlowName) {
    }

}
