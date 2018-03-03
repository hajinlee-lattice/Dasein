package com.latticeengines.workflow.exposed.build;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.workflow.core.WorkflowTranslator;

public abstract class AbstractWorkflow<T> extends AbstractNameAwareBean {

    public abstract Workflow defineWorkflow(T workflowConfig);

    @Autowired
    private WorkflowTranslator workflowTranslator;

    public Job buildWorkflow(T workflowConfig) throws Exception {
        return workflowTranslator.buildWorkflow(name(), defineWorkflow(workflowConfig));
    }

}
