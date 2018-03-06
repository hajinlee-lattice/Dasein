package com.latticeengines.cdl.workflow.steps;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.MatchCdlStepConfiguration;
import com.latticeengines.workflow.exposed.build.BaseWorkflowStep;

@Component("matchCdlOrchestratorStep")
public class MatchCdlOrchestratorStep extends BaseWorkflowStep<MatchCdlStepConfiguration> {

    private static Logger log = LoggerFactory.getLogger(MatchCdlOrchestratorStep.class);

    @Override
    public void execute() {
        int count = 0;
        Table accountTable = getObjectFromContext(CUSTOM_EVENT_IMPORT_ACCOUNT_ID, Table.class);
        if (accountTable != null) {
            putObjectInContext(CUSTOM_EVENT_MATCH_ACCOUNT_ID, accountTable);
            count++;
        }

        Table noAccountTable = getObjectFromContext(CUSTOM_EVENT_IMPORT_WITHOUT_ACCOUNT_ID, Table.class);
        if (noAccountTable != null) {
            putObjectInContext(CUSTOM_EVENT_MATCH_WITHOUT_ACCOUNT_ID, accountTable);
            count++;
        }
        if (count < 1) {
            throw new RuntimeException("There's no import data to be matched.");
        }
    }

}
