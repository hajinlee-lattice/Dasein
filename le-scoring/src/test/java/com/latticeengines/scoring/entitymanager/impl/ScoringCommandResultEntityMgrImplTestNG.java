package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
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

public class ScoringCommandResultEntityMgrImplTestNG extends ScoringFunctionalTestNGBase {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @BeforeMethod(groups = "functional", enabled = false)
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional", enabled = false)
    public void setup() throws Exception {
    }

    @Test(groups = "functional", enabled = false)
    public void testFindByScoringCommandState() throws Exception {
        ScoringCommand command = new ScoringCommand("Nutanix", ScoringCommandStatus.NEW, "Q_Table_Nutanix", 0, 100,
                Timestamp.valueOf("2015-04-28 00:00:00"));
        scoringCommandEntityMgr.create(command);

        ScoringCommandResult result = new ScoringCommandResult("Nutanix", ScoringCommandStatus.NEW, "Q_Table_Nutanix",
                100, Timestamp.valueOf("2015-04-28 00:00:01"));
        scoringCommandResultEntityMgr.create(result);

        ScoringCommandState state = new ScoringCommandState(command, ScoringCommandStep.EXPORT_DATA);
        state.setLeadOutputQueuePid(result.getPid());
        scoringCommandStateEntityMgr.create(state);

        ScoringCommandResult retrieved = scoringCommandResultEntityMgr.findByKey(scoringCommandStateEntityMgr
                .findByScoringCommandAndStep(command, ScoringCommandStep.EXPORT_DATA).getLeadOutputQueuePid());
        assertNotNull(retrieved);
        assertEquals(retrieved.getId(), result.getId());

        assertEquals(scoringCommandResultEntityMgr.getConsumed().size(), 0);
        retrieved.setPopulated(Timestamp.valueOf("2015-04-28 00:00:02"));
        retrieved.setStatus(ScoringCommandStatus.POPULATED);
        scoringCommandResultEntityMgr.update(retrieved);
        assertEquals(scoringCommandResultEntityMgr.getConsumed().size(), 0);

        retrieved.setConsumed(Timestamp.valueOf("2015-04-28 00:00:03"));
        retrieved.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandResultEntityMgr.update(retrieved);
        assertEquals(scoringCommandResultEntityMgr.getConsumed().size(), 1);
    }
}
