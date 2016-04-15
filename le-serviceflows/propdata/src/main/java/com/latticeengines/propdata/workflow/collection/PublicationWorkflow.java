package com.latticeengines.propdata.workflow.collection;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.workflow.collection.steps.Publish;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("publicationWorkflow")
public class PublicationWorkflow  extends AbstractWorkflow<PublicationWorkflowConfiguration>  {

    @Autowired
    private Publish publish;

    @Bean
    public Job publicationWorkflow() throws Exception {
        return publicationWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(publish) //
                .build();
    }

}
