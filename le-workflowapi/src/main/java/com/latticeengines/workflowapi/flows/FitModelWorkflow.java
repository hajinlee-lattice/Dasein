package com.latticeengines.workflowapi.flows;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;
import com.latticeengines.workflow.exposed.build.WorkflowConfiguration;
import com.latticeengines.workflowapi.steps.prospectdiscovery.CreateEventTableFromMatchResult;
import com.latticeengines.workflowapi.steps.prospectdiscovery.ImportData;
import com.latticeengines.workflowapi.steps.prospectdiscovery.LoadHdfsTableToPDServer;
import com.latticeengines.workflowapi.steps.prospectdiscovery.Match;
import com.latticeengines.workflowapi.steps.prospectdiscovery.ProfileAndModel;
import com.latticeengines.workflowapi.steps.prospectdiscovery.RunDataFlow;
import com.latticeengines.workflowapi.steps.prospectdiscovery.Sample;

@Component("fitModelWorkflow")
public class FitModelWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private ImportData importData;

    @Autowired
    private RunDataFlow runDataFlow;

    @Autowired
    private LoadHdfsTableToPDServer loadHdfsTableToPDServer;

    @Autowired
    private Match match;

    @Autowired
    private CreateEventTableFromMatchResult createEventTableFromMatchResult;

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
                .next(loadHdfsTableToPDServer) //
                .next(match) //
                .next(createEventTableFromMatchResult) //
                .next(sample) //
                .next(profileAndModel) //.
                .build();
    }

}
