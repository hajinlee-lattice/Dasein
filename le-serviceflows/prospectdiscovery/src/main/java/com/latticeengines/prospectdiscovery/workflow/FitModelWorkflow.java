package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.modeling.ActivateModel;
import com.latticeengines.modeling.workflow.steps.modeling.ChooseModel;
import com.latticeengines.modeling.workflow.steps.modeling.CreateModel;
import com.latticeengines.modeling.workflow.steps.modeling.InvokeDataScienceAnalysis;
import com.latticeengines.modeling.workflow.steps.modeling.Profile;
import com.latticeengines.modeling.workflow.steps.modeling.ReviewModel;
import com.latticeengines.modeling.workflow.steps.modeling.Sample;
import com.latticeengines.modeling.workflow.steps.modeling.SetMatchSelection;
import com.latticeengines.modeling.workflow.steps.modeling.WriteMetadataFiles;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateAttributeLevelSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateImportSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreatePreMatchEventTable;
import com.latticeengines.prospectdiscovery.workflow.steps.MarkReportOutOfDate;
import com.latticeengines.scoring.workflow.steps.Score;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("fitModelWorkflow")
public class FitModelWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private MarkReportOutOfDate markReportOutOfDate;

    @Autowired
    private ImportData importData;

    @Autowired
    private CreatePreMatchEventTable createPreMatchEventTable;

    @Autowired
    private MatchWorkflow matchWorkflow;

    @Autowired
    private CreateImportSummaryWorkflow createImportSummaryWorkflow;

    @Autowired
    private Sample sample;

    @Autowired
    private SetMatchSelection setMatchSelection;

    @Autowired
    private WriteMetadataFiles writeMetadataFiles;

    @Autowired
    private Profile profile;

    @Autowired
    private ReviewModel reviewModel;

    @Autowired
    private CreateModel createModel;

    @Autowired
    private InvokeDataScienceAnalysis invokeDataScienceAnalysis;

    @Autowired
    private ChooseModel chooseModel;

    @Autowired
    private ActivateModel activateModel;

    @Autowired
    private Score score;

    @Autowired
    private CreateAttributeLevelSummaryWorkflow createAttributeLevelSummaryWorkflow;

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder().next(markReportOutOfDate) //
                .next(importData) //
                .next(createPreMatchEventTable) //
                .next(matchWorkflow, null) //
                .next(createImportSummaryWorkflow, null) //
                .next(sample) //
                .next(setMatchSelection) //
                .next(writeMetadataFiles) //
                .next(profile) //
                .next(reviewModel) //
                .next(createModel) //
                .next(chooseModel) //
                .next(activateModel)//
                .next(invokeDataScienceAnalysis)//
                .next(score) //
                .next(createAttributeLevelSummaryWorkflow, null) //
                .build();
    }

}
