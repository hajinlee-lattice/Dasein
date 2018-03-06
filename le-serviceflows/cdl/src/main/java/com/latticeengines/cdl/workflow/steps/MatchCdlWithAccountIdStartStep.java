package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithAccountIdWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdStartStep")
public class MatchCdlWithAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, Table.class);
        if (inputTable == null) {
            log.info("There's no table with account Id, skip the workflow.");
            skipEmbeddedWorkflow(MatchCdlWithAccountIdWorkflowConfiguration.class);
        } else {
            enableEmbeddedWorkflow(MatchCdlWithAccountIdWorkflowConfiguration.class);
        }

    }

}
