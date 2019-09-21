package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringCommandLogEntityMgrImplTestNG extends ScoringFunctionalTestNGBase{

    @Autowired
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @BeforeMethod(groups = "functional", enabled = false)
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testFindByModelCommand() throws Exception {
        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, Timestamp.valueOf("2015-04-28 00:00:00"));
        scoringCommandEntityMgr.create(scoringCommand);

        ScoringCommand anotherScoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.NEW, "Q_Table_Nutanix", 0, 100, Timestamp.valueOf("2015-04-28 00:00:01"));
        scoringCommandEntityMgr.create(anotherScoringCommand);

        for (int i = 0; i < 3; i++) {
            scoringCommandLogEntityMgr.create(new ScoringCommandLog(scoringCommand, "a message" + i));
        }

        scoringCommandLogEntityMgr.create(new ScoringCommandLog(anotherScoringCommand, "a message"));

        assertEquals(scoringCommandLogEntityMgr.findByScoringCommand(scoringCommand).size(), 3);
        assertEquals(scoringCommandLogEntityMgr.findByScoringCommand(anotherScoringCommand).size(), 1);
    }
}
