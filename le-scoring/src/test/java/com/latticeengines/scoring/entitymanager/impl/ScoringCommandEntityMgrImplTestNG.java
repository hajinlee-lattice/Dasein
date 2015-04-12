package com.latticeengines.scoring.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import java.sql.Timestamp;
import java.util.List;

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

    @BeforeMethod(groups = "functional")
    public void beforeMethod() {
    }

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        scoringCommandEntityMgr.deleteAll();
    }

    @Test(groups = "functional")
    public void testGetNewAndInProgress() throws Exception {
        List<ScoringCommand> commands = scoringCommandEntityMgr.getPopulated();
        assertEquals(commands.size(), 0);

        ScoringCommand scoringCommand = new ScoringCommand(1L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(scoringCommand);
        commands = scoringCommandEntityMgr.getPopulated();
        assertEquals(commands.size(), 1);

        ScoringCommand secondScoringCommand = new ScoringCommand(2L, "Nutanix", ScoringCommandStatus.NEW, "Q_Table_Nutanix", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(secondScoringCommand);
        commands = scoringCommandEntityMgr.getPopulated();
        assertEquals(commands.size(), 1);

        secondScoringCommand.setStatus(ScoringCommandStatus.POPULATED);
        scoringCommandEntityMgr.createOrUpdate(secondScoringCommand);
        assertEquals(commands.size(), 1);

        ScoringCommand anotherScoringCommand = new ScoringCommand(3L, "MuleSoft", ScoringCommandStatus.POPULATED, "Q_Table_MuleSoft", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(anotherScoringCommand);
        commands = scoringCommandEntityMgr.getPopulated();
        assertEquals(commands.size(), 2);

        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.createOrUpdate(scoringCommand);
        assertEquals(commands.size(), 2);

        ScoringCommand lastScoringCommand = new ScoringCommand(4L, "Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandEntityMgr.create(lastScoringCommand);
        commands = scoringCommandEntityMgr.getPopulated();
        assertEquals(commands.size(), 2);

        for(ScoringCommand command : commands){
            assertTrue(command.getPid() == 2L || command.getPid() == 3L);
        }
    }
}
