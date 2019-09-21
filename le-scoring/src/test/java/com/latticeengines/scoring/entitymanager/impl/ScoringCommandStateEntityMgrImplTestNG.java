package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringCommandStateEntityMgrImplTestNG extends ScoringFunctionalTestNGBase{

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @BeforeMethod(groups = "functional", enabled = false)
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testFindLastStateByScoringCommand(){
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, Timestamp.valueOf("2015-04-28 00:00:00"));
        scoringCommandEntityMgr.create(scoringCommand);

        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 0);

        ScoringCommandState scoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.LOAD_DATA);
        scoringCommandStateEntityMgr.create(scoringCommandState);
        
        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 1);
        assertEquals(scoringCommandStateEntityMgr.findLastStateByScoringCommand(scoringCommand).getScoringCommandStep(), ScoringCommandStep.LOAD_DATA);

        ScoringCommandState secondScoringCommandState = new ScoringCommandState(scoringCommand, ScoringCommandStep.SCORE_DATA);
        scoringCommandStateEntityMgr.create(secondScoringCommandState);

        assertEquals(scoringCommandStateEntityMgr.findAll().size(), 2);
        assertEquals(scoringCommandStateEntityMgr.findLastStateByScoringCommand(scoringCommand).getScoringCommandStep(), ScoringCommandStep.SCORE_DATA);
    }
}
