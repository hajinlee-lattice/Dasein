package com.latticeengines.workflow.library;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractWorkflow;
import com.latticeengines.workflow.build.WorkflowBuilder;
import com.latticeengines.workflow.core.Workflow;
import com.latticeengines.workflow.steps.ModelGenerateSamples;
import com.latticeengines.workflow.steps.ModelLoadData;
import com.latticeengines.workflow.steps.ModelProfileData;
import com.latticeengines.workflow.steps.ModelSubmit;

@Component("modelWorkflow")
public class ModelWorkflow extends AbstractWorkflow {

    @Autowired
    private ModelLoadData modelLoadData;

    @Autowired
    private ModelGenerateSamples modelGenerateSamples;

    @Autowired
    private ModelProfileData modelProfileData;

    @Autowired
    private ModelSubmit modelSubmit;

    @Bean
    public Job buildModelWorkflow() throws Exception {
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
