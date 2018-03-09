package com.latticeengines.modeling.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.modeling.PMMLModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.CreatePMMLModel;
import com.latticeengines.modeling.workflow.steps.modeling.DownloadAndProcessModelSummaries;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("pmmlModelWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class PMMLModelWorkflow extends AbstractWorkflow<PMMLModelWorkflowConfiguration> {

    @Inject
    private CreatePMMLModel createPMMLModel;

    @Inject
    private DownloadAndProcessModelSummaries downloadAndProcessModelSummaries;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(PMMLModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(createPMMLModel) //
                .next(downloadAndProcessModelSummaries) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
