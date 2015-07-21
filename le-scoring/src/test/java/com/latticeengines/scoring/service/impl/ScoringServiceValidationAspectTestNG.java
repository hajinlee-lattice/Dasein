package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
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
import com.latticeengines.scoring.service.ScoringStepProcessor;

public class ScoringServiceValidationAspectTestNG extends ScoringFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ScoringServiceValidationAspectTestNG.class);

    private static final String customer = "Nutanix";

    @Value("${scoring.test.table}")
    private String testInputTable;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringStepProcessor scoringStepFinishProcessor;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
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
