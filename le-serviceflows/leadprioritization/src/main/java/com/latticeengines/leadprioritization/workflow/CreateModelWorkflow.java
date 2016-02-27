package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.CreateEventTableReport;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTable;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchWorkflow;
import com.latticeengines.serviceflows.workflow.modeling.ActivateModel;
import com.latticeengines.serviceflows.workflow.modeling.ProfileAndModel;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("createModelWorkflow")
public class CreateModelWorkflow extends AbstractWorkflow<CreateModelWorkflowConfiguration> {
    @Autowired
    private ImportData importData;

    @Autowired
    private CreateEventTableReport createEventTableReport;

    @Autowired
    private DedupEventTable dedupEventTable;

    @Autowired
    private MatchWorkflow matchWorkflow;

    @Autowired
    private Sample sample;

    @Autowired
    private ProfileAndModel profileAndModel;

    @Autowired
    private ActivateModel activateModel;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job createModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(importData) //
                .next(createEventTableReport) //
                .next(dedupEventTable) //
                .next(matchWorkflow) //
                .next(sample) //
                .next(profileAndModel) //
                .next(activateModel) //
                .listener(sendEmailAfterModelCompletionListener)//
                .build();
    }
}
