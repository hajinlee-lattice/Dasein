package com.latticeengines.workflow.exposed.build;

public interface WorkflowInterface<T> {

    Workflow defineWorkflow(T config);

    String name();
}
