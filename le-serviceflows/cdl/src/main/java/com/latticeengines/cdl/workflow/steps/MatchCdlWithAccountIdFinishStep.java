package com.latticeengines.cdl.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdFinishStep")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class MatchCdlWithAccountIdFinishStep extends BaseWorkflowStep<MatchCdlAccountConfiguration> {

    @Override
    public void execute() {
        removeObjectFromContext(MATCH_FETCH_ONLY);
        Table eventTable = getObjectFromContext(EVENT_TABLE, Table.class);
        if (eventTable != null) {
            putObjectInContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, eventTable);
            removeObjectFromContext(EVENT_TABLE);
        }
    }
}
