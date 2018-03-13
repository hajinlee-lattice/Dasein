package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdSimpleStartStep")
public class MatchCdlWithAccountIdSimpleStartStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        Table inputTable = getObjectFromContext(CUSTOM_EVENT_MATCH_ACCOUNT, Table.class);
        putObjectInContext(PREMATCH_UPSTREAM_EVENT_TABLE, inputTable);
        putStringValueInContext(MATCH_FETCH_ONLY, "true");

    }

}
