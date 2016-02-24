package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.batch.core.Job;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.prospectdiscovery.workflow.steps.CreateImportSummaryWorkflow;
import com.latticeengines.prospectdiscovery.workflow.steps.CreatePreMatchEventTable;
import com.latticeengines.prospectdiscovery.workflow.steps.MarkReportOutOfDate;
import com.latticeengines.prospectdiscovery.workflow.steps.MockCreateAttributeLevelSummaryWorkflow;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MockMatchWorkflow;
import com.latticeengines.serviceflows.workflow.modeling.ChooseModel;
import com.latticeengines.serviceflows.workflow.modeling.MockProfileAndModel;
import com.latticeengines.serviceflows.workflow.modeling.Sample;
import com.latticeengines.serviceflows.workflow.scoring.Score;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockFitModelWorkflow")
public class MockFitModelWorkflow extends AbstractWorkflow<WorkflowConfiguration> {

    @Autowired
    private MarkReportOutOfDate markReportOutOfDate;

    @Autowired
    private ImportData importData;

    @Autowired
    private CreatePreMatchEventTable createPreMatchEventTable;

    @Autowired
    private MockMatchWorkflow mockMatchWorkflow;

    @Autowired
    private CreateImportSummaryWorkflow createImportSummaryWorkflow;

    @Autowired
    private Sample sample;

    @Autowired
    private MockProfileAndModel mockProfileAndModel;

    @Autowired
    private ChooseModel chooseModel;

    @Autowired
    private Score score;

    @Autowired
    private MockCreateAttributeLevelSummaryWorkflow mockCreateAttributeLevelSummaryWorkflow;

    @Bean
    public Job mockFitModelWorkflowJob() throws Exception {
        return buildWorkflow();
    }
    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder().next(markReportOutOfDate) //
                .next(importData) //
                .next(createPreMatchEventTable) //
                .next(mockMatchWorkflow) //
                .next(createImportSummaryWorkflow) //
                .next(sample) //
                .next(mockProfileAndModel) //
                .next(chooseModel) //
                .next(score) //
                .next(mockCreateAttributeLevelSummaryWorkflow) //
                .build();
    }

}
