package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringCommandEntityMgrImplTestNG extends ScoringFunctionalTestNGBase{

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @BeforeMethod(groups = "functional", enabled = false)
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testGetPopulatedAndConsumed() throws Exception {
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 0);

        ScoringCommand scoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 1);

        ScoringCommand secondScoringCommand = new ScoringCommand("Nutanix", ScoringCommandStatus.NEW, "Q_Table_Nutanix", 100, 200, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(secondScoringCommand);
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 1);

        secondScoringCommand.setStatus(ScoringCommandStatus.POPULATED);
        scoringCommandEntityMgr.createOrUpdate(secondScoringCommand);
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 2);

        ScoringCommand lastScoringCommand = new ScoringCommand("MuleSoft", ScoringCommandStatus.POPULATED, "Q_Table_MuleSoft", 10, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(lastScoringCommand);
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 3);

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.createOrUpdate(scoringCommand);
        assertEquals(scoringCommandEntityMgr.getPopulated().size(), 2);

        assertEquals(scoringCommandEntityMgr.getConsumed().size(), 1);
        scoringCommandEntityMgr.delete(scoringCommand);
        assertEquals(scoringCommandEntityMgr.getConsumed().size(), 0);
    }
}
