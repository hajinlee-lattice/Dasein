package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlAccountConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithoutAccountIdFinishStep")
public class MatchCdlWithoutAccountIdFinishStep extends BaseWorkflowStep<MatchCdlAccountConfiguration> {

    @Override
    public void execute() {
        removeObjectFromContext(MATCH_FETCH_ONLY);
        Table eventTable = getObjectFromContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, Table.class);
        if (eventTable != null) {
            putObjectInContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, eventTable);
            removeObjectFromContext(EVENT_TABLE);
        }
    }

}
