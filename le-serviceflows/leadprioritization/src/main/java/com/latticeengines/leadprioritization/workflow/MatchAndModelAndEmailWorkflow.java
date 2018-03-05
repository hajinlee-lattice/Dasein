package com.latticeengines.leadprioritization.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.ResolveMetadataFromUserRefinedAttributes;
import com.latticeengines.modeling.workflow.ModelWorkflow;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.modeling.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.scoring.workflow.RTSBulkScoreWorkflow;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("modelAndEmailWorkflow")
@Lazy
public class MatchAndModelAndEmailWorkflow extends AbstractWorkflow<MatchAndModelWorkflowConfiguration> {

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private ResolveMetadataFromUserRefinedAttributes resolveMetadataFromUserRefinedAttributes;

    @Inject
    private ModelWorkflow modelWorkflow;

    @Autowired
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private RTSBulkScoreWorkflow rtsBulkScoreWorkflow;

    @Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;

    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(MatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(matchDataCloudWorkflow, null) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(resolveMetadataFromUserRefinedAttributes) //
                .next(modelWorkflow, null) //
                .next(setConfigurationForScoring) //
                .next(rtsBulkScoreWorkflow, null) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
