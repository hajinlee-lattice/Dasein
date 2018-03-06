package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithoutAccountIdWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithoutAccountIdStartStep")
public class MatchCdlWithoutAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, Table.class);
        if (inputTable == null) {
            log.info("There's no table with account Id, skip the workflow.");
            skipEmbeddedWorkflow(MatchCdlWithoutAccountIdWorkflowConfiguration.class);
        } else {
            enableEmbeddedWorkflow(MatchCdlWithoutAccountIdWorkflowConfiguration.class);
            putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
        }

    }

}
