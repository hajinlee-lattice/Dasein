package com.latticeengines.leadprioritization.workflow;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.leadprioritization.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.leadprioritization.workflow.steps.CreatePMMLModel;
import com.latticeengines.serviceflows.workflow.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("pmmlModelWorkflow")
public class PMMLModelWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Autowired
    private CreatePMMLModel createPMMLModel;

    @Autowired
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Bean
    public Job pmmlModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(createPMMLModel) //
                .next(downloadAndProcessModelSummaries) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
