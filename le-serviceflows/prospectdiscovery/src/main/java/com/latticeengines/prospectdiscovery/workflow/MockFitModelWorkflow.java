package com.latticeengines.prospectdiscovery.workflow;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.modeling.workflow.steps.modeling.ChooseModel;
import com.latticeengines.modeling.workflow.steps.modeling.MockProfileAndModel;
import com.latticeengines.modeling.workflow.steps.modeling.Sample;
import com.latticeengines.prospectdiscovery.workflow.steps.CreatePreMatchEventTable;
import com.latticeengines.prospectdiscovery.workflow.steps.MarkReportOutOfDate;
import com.latticeengines.scoring.workflow.steps.Score;
import com.latticeengines.serviceflows.workflow.importdata.ImportData;
import com.latticeengines.serviceflows.workflow.match.MockMatchWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("mockFitModelWorkflow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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

    @Override
    public Workflow defineWorkflow(WorkflowConfiguration config) {
        return new WorkflowBuilder(name()) //
                .next(markReportOutOfDate) //
                .next(importData) //
                .next(createPreMatchEventTable) //
                .next(mockMatchWorkflow, null) //
                .next(createImportSummaryWorkflow, null) //
                .next(sample) //
                .next(mockProfileAndModel) //
                .next(chooseModel) //
                .next(score) //
                .next(mockCreateAttributeLevelSummaryWorkflow, null) //
                .build();
    }

}
