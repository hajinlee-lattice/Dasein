package com.latticeengines.workflow.build;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.workflow.core.Workflow;
import com.latticeengines.workflow.core.WorkflowTranslator;

public abstract class AbstractWorkflow extends AbstractNameAwareBean {

    public abstract Workflow defineWorkflow();

    @Autowired
    private WorkflowTranslator workflowTranslator;

    protected Job buildWorkflow() throws Exception {
        return workflowTranslator.buildWorkflow(name(), defineWorkflow());
    }

}
