package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithAccountIdWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdStartStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, Table.class);
        if (inputTable == null || inputTable.getCount() == 0) {
            log.info("There's no data with account Id, skip the workflow.");
            skipEmbeddedWorkflow(getParentNamespace(), MatchCdlWithAccountIdWorkflowConfiguration.class);
        } else {
            putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
            putStringValueInContext(MATCH_FETCH_ONLY, "true");
            enableEmbeddedWorkflow(getParentNamespace(), MatchCdlWithAccountIdWorkflowConfiguration.class);
        }

    }

}
