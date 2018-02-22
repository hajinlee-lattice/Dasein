package com.latticeengines.cdl.workflow;

import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;

@Component("matchCDLWorkflow")
@Lazy
public class MatchCDLWorkflow extends AbstractWorkflow<Object> {

    @Override
    public Workflow defineWorkflow() {
        // TODO Auto-generated method stub
        return null;
    }

}
