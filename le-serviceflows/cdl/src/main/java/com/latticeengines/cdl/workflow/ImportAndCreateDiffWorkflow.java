package com.latticeengines.cdl.workflow;

import com.latticeengines.domain.exposed.serviceflows.cdl.ImportAndCreateDiffWorkflowConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.match.MatchDataCloudWorkflow;
import com.latticeengines.workflow.exposed.build.AbstractWorkflow;
import com.latticeengines.workflow.exposed.build.Workflow;
import com.latticeengines.workflow.exposed.build.WorkflowBuilder;

@Component("importAndCreateDiffWorkflow")
public class ImportAndCreateDiffWorkflow extends AbstractWorkflow<ImportAndCreateDiffWorkflowConfiguration> {

    @Autowired
    private CDLImportWorkflow cdlImportWorkflow;

    @Autowired
    private MatchDataCloudWorkflow matchDataCloudWorkflow;

    @Override
    public Workflow defineWorkflow() {
        return new WorkflowBuilder() //
                .next(cdlImportWorkflow) //
                .next(matchDataCloudWorkflow) //
                .build();
    }

}
