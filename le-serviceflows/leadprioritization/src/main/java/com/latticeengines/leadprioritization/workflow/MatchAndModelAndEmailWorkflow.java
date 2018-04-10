package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.ModelWorkflow;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.modeling.workflow.steps.ResolveMetadataFromUserRefinedAttributes;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.steps.ComputeLiftDataFlow;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelAndEmailWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchAndModelAndEmailWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private ResolveMetadataFromUserRefinedAttributes resolveMetadataFromUserRefinedAttributes;

    @Inject
    private ModelWorkflow modelWorkflow;

    @Inject
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private ComputeLiftDataFlow computeLift;

    @Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;

    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(MatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(matchDataCloudWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(resolveMetadataFromUserRefinedAttributes) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(rtsBulkScoreWorkflow) //
                .next(computeLift) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
