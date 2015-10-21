package com.latticeengines.workflow.build;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.workflow.core.Workflow;
import com.latticeengines.workflow.core.WorkflowTranslator;

public abstract class AbstractWorkflow {

    public abstract Workflow defineWorkflow();

    @Autowired
    private WorkflowTranslator workflowTranslator;

    protected Job buildWorkflow() throws Exception {
        String name = getClass().getSimpleName();
        int index = name.indexOf("$$");
        if (index > -1) {
            name = name.substring(0, name.indexOf("$$"));
        }

        return workflowTranslator.buildWorkflow(name, defineWorkflow());
    }

}
