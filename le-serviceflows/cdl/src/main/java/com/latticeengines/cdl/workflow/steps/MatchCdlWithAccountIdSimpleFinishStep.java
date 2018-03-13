package com.latticeengines.cdl.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlWithAccountIdSimpleFinishStep")
public class MatchCdlWithAccountIdSimpleFinishStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    @Override
    public void execute() {
        removeObjectFromContext(MATCH_FETCH_ONLY);

    }

}
