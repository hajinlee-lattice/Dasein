package com.latticeengines.cdl.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableFilterStep;
import com.latticeengines.cdl.workflow.steps.CreateCdlEventTableStep;
import com.latticeengines.cdl.workflow.steps.SetCdlConfigurationForScoring;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.modeling.workflow.listeners.SendEmailAfterModelCompletionListener;
import com.latticeengines.scoring.workflow.steps.PivotScoreAndEventDataFlow;
import com.latticeengines.serviceflows.workflow.export.ExportData;
import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("ratingEngineImportMatchAndModelWorkflow")
@Lazy
public class RatingEngineImportMatchAndModelWorkflow
        extends AbstractWorkflow<RatingEngineImportMatchAndModelWorkflowConfiguration> {

    @Autowired
    private CreateCdlEventTableFilterStep createCdlEventTableFilterStep;

    @Autowired
    private CreateCdlEventTableStep createCdlEventTableStep;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Autowired
    private CdlModelWorkflow modelWorkflow;

    @Autowired
    private SetCdlConfigurationForScoring setCdlConfigurationForScoring;

    @Autowired
    private RatingEngineScoreWorkflow scoreWorkflow;

    @Autowired
    private PivotScoreAndEventDataFlow pivotScoreAndEventDataFlow;

    @Autowired
    private ExportData exportData;

    @Autowired
    private SendEmailAfterModelCompletionListener sendEmailAfterModelCompletionListener;

    @Override
    public Workflow defineWorkflow(RatingEngineImportMatchAndModelWorkflowConfiguration config) {
        return new WorkflowBuilder() //
                .next(createCdlEventTableFilterStep) //
                .next(createCdlEventTableStep) //
                .next(matchDataCloudWorkflow, null) //
                .next(modelWorkflow, null) //
                .next(setCdlConfigurationForScoring) //
                .next(scoreWorkflow, null) //
                .next(pivotScoreAndEventDataFlow) //
                .next(exportData) //
                .listener(sendEmailAfterModelCompletionListener) //
                .build();
    }
}
