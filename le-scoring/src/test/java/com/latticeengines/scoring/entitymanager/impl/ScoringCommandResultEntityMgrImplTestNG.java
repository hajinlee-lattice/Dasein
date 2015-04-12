package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.functionalframework.ScoringFunctionalTestNGBase;

public class ScoringCommandResultEntityMgrImplTestNG extends ScoringFunctionalTestNGBase{

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandLogEntityMgr scoringCommandLogEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        scoringCommandLogEntityMgr.deleteAll();
        scoringCommandEntityMgr.deleteAll();
        scoringCommandResultEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    public void testFindByModelCommand() throws Exception {
        ScoringCommand command = new ScoringCommand(1L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, Timestamp.valueOf("2015-04-28 00:00:00"));
        scoringCommandEntityMgr.create(command);

        assertNull(scoringCommandResultEntityMgr.findByScoringCommand(command));

        ScoringCommandResult result = new ScoringCommandResult(2L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 100, Timestamp.valueOf("2015-04-28 00:00:01"));
        scoringCommandResultEntityMgr.create(result);

        ScoringCommandResult retrieved = scoringCommandResultEntityMgr.findByScoringCommand(command);
        assertNotNull(retrieved);
        assertEquals(retrieved.getId(), command.getId());

        retrieved.setConsumed(Timestamp.valueOf("2015-04-28 00:00:02"));
        retrieved.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandResultEntityMgr.update(retrieved);
    }
}
