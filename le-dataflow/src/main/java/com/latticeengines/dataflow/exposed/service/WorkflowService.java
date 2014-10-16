package com.latticeengines.dataflow.exposed.service;

public interface WorkflowService {

    void executeNamedOrchestrationFlow(String orchestrationFlowName);
}
