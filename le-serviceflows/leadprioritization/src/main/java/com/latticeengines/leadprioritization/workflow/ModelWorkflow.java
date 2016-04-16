package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.modeling.ActivateModel;
import com.latticeengines.serviceflows.workflow.modeling.ProfileAndModel;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelWorkflow")
public class ModelWorkflow extends AbstractWorkflow<ModelWorkflowConfiguration> {

    @Autowired
    private Sample sample;

    @Autowired
    private ExportData exportData;

    @Autowired
    private ProfileAndModel profileAndModel;

    @Autowired
    private ActivateModel activateModel;

    @Bean
    public Job modelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(sample) //
                .next(exportData) //
                .next(profileAndModel) //
                .next(activateModel) //
                .build();

    }
}
