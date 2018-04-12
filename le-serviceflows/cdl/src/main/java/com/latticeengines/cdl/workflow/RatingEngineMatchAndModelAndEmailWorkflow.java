package com.latticeengines.cdl.workflow;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.listeners.ActivateRatingEngineListener;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.modeling.workflow.steps.DedupEventTable;
import com.latticeengines.modeling.workflow.steps.ResolveMetadataFromUserRefinedAttributes;
import com.latticeengines.scoring.dataflow.ComputeLift;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.scoring.workflow.steps.SetConfigurationForScoring;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.serviceflows.workflow.transformation.AddStandardAttributes;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineModelAndEmailWorkflow")
@Lazy
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RatingEngineMatchAndModelAndEmailWorkflow
        extends AbstractWorkflow<RatingEngineMatchAndModelWorkflowConfiguration> {

    @Inject
    private DedupEventTable dedupEventTableDataFlow;

    @Inject
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Inject
    private AddStandardAttributes addStandardAttributesDataFlow;

    @Inject
    private ResolveMetadataFromUserRefinedAttributes resolveMetadataFromUserRefinedAttributes;

    @Inject
    private CdlModelWorkflow modelWorkflow;

    @Autowired
    private SetConfigurationForScoring setConfigurationForScoring;

    @Inject
    private RatingEngineScoreWorkflow scoreWorkflow;

    @Inject
    private ComputeLift computeLift;

    @Inject
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;

    @Inject
    private ExportData exportData;

    @Inject
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Inject
    private ActivateRatingEngineListener activateRatingEngineListener;

    @Override
    public Workflow defineWorkflow(RatingEngineMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder(name(), config) //
                .next(matchDataCloudWorkflow) //
                .next(dedupEventTableDataFlow) //
                .next(addStandardAttributesDataFlow) //
                .next(resolveMetadataFromUserRefinedAttributes) //
                .next(modelWorkflow) //
                .next(setConfigurationForScoring) //
                .next(scoreWorkflow) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(activateRatingEngineListener) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
