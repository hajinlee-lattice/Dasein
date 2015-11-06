package com.latticeengines.workflowapi.flows;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelGenerateSamples;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelLoadData;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelProfileData;
import com.latticeengines.workflowapi.steps.dlorchestration.ModelSubmit;

@Component("modelWorkflow")
public class ModelWorkflow extends AbstractWorkflow<ModelWorkflowConfiguration> {

    @Autowired
    private ModelLoadData modelLoadData;

    @Autowired
    private ModelGenerateSamples modelGenerateSamples;

    @Autowired
    private ModelProfileData modelProfileData;

    @Autowired
    private ModelSubmit modelSubmit;

    @Bean
    public Job modelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(modelLoadData) //
                .next(modelGenerateSamples) //
                .next(modelProfileData) //
                .next(modelSubmit) //
                .build();
    }

}
