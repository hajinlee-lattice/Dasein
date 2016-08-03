package com.latticeengines.leadprioritization.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.AddStandardAttributesViaJavaFunction;
import com.latticeengines.leadprioritization.workflow.steps.DedupEventTable;
import com.latticeengines.leadprioritization.workflow.steps.ResolveMetadataFromUserRefinedAttributes;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelAndEmailWorkflow")
public class MatchAndModelAndEmailWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Autowired
    private DedupEventTable dedupEventTable;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private AddStandardAttributesViaJavaFunction addStandardAttributesViaJavaFunction;

    @Autowired
    protected ResolveMetadataFromUserRefinedAttributes resolveMetadataFromUserRefinedAttributes;

    @Autowired
    private ModelWorkflow modelWorkflow;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job modelAndEmailWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(dedupEventTable) //
                .next(matchDataCloudWorkflow) //
                .next(addStandardAttributesViaJavaFunction) //
                .next(resolveMetadataFromUserRefinedAttributes) //
                .next(modelWorkflow) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
