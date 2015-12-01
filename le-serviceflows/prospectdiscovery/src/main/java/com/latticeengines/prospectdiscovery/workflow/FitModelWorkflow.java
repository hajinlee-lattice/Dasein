package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchWorkflow;
import com.latticeengines.serviceflows.workflow.modeling.ProfileAndModel;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("fitModelWorkflow")
public class FitModelWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private ImportData importData;

    @Autowired
    private RunDataFlow runDataFlow;

    @Autowired
    private MatchWorkflow matchWorkflow;

    @Autowired
    private Sample sample;

    @Autowired
    private ProfileAndModel profileAndModel;

    @Bean
    public Job fitModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(importData) //
                .next(runDataFlow) //
                .next(matchWorkflow) //
                .next(sample) //
                .next(profileAndModel) //
                .build();
    }

}
