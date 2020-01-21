package com.latticeengines.scoring.orchestration.service.impl;

import java.sql.Timestamp;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;
public class ScoringServiceValidationAspectTestNG extends ScoringFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ScoringServiceValidationAspectTestNG.class);

    private static final String customer = "Nutanix";

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Inject
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Inject
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Inject
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Inject
    private ScoringServiceValidationAspect scoringServiceValidationAspect;
    
    @Test(groups = "functional", enabled = false)
    public void testValidationScoreResult() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand(customer, ScoringCommandStatus.POPULATED, testInputTable,
                0, 4352, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        ScoringCommandResult scoringCommandResult = new ScoringCommandResult(customer, ScoringCommandStatus.NEW, "Leads_Prod_Results", 4352, new Timestamp(System.currentTimeMillis()));
        scoringCommandResultEntityMgr.create(scoringCommandResult);
        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        scoringCommandState.setLeadOutputQueuePid(scoringCommandResult.getPid());
        scoringCommandStateEntityMgr.create(scoringCommandState);

        scoringServiceValidationAspect.validateScoreResult(scoringCommand);
    }
}
