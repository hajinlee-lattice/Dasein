package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.MatchCdlWithoutAccountIdWorkflowConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithoutAccountIdStartStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithoutAccountIdStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, Table.class);
        if (inputTable == null || inputTable.getCount() == 0) {
            log.info("There's no data without account Id, skip the workflow.");
            skipEmbeddedWorkflow(getParentNamespace(), MatchCdlWithoutAccountIdWorkflowConfiguration.class);
        } else {
            enableEmbeddedWorkflow(getParentNamespace(), MatchCdlWithoutAccountIdWorkflowConfiguration.class);
            putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
        }

    }

}
