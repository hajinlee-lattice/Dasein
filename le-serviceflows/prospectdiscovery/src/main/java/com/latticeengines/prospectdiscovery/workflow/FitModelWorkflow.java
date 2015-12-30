package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateAttributeLevelSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateImportSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreatePreMatchEventTable;
import com.latticeengines.prospectdiscovery.workflow.steps.MarkReportOutOfDate;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MatchWorkflow;
import com.latticeengines.serviceflows.workflow.modeling.ChooseModel;
import com.latticeengines.serviceflows.workflow.modeling.ProfileAndModel;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.serviceflows.workflow.scoring.Score;
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
    private ProfileAndModel profileAndModel;

    @Autowired
    private ChooseModel chooseModel;

    @Autowired
    private Score score;
    
    @Autowired
    private CreateAttributeLevelSummaryWorkflow createAttributeLevelSummaryWorkflow;

    @Bean
    public Job fitModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(markReportOutOfDate) //
                .next(importData) //
                .next(createPreMatchEventTable) //
                .next(matchWorkflow) //
                .next(createImportSummaryWorkflow) //
                .next(sample) //
                .next(profileAndModel) //
                .next(chooseModel) //
                .next(score) //
                .next(createAttributeLevelSummaryWorkflow) //
                .build();
    }

}
