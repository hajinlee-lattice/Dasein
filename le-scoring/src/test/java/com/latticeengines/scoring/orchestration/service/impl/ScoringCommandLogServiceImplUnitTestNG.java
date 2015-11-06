package com.latticeengines.scoring.service.impl;

import static org.mockito.Mockito.mock;
import static org.mockito.MockitoAnnotations.initMocks;

import java.sql.Timestamp;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;
import com.latticeengines.scoring.entitymanager.impl.ScoringCommandLogEntityMgrImpl;

public class ScoringCommandLogServiceImplUnitTestNG {

    ScoringCommandLogServiceImpl scoringCommandLogServiceImpl = new ScoringCommandLogServiceImpl();
    
    @BeforeClass(groups = "unit")
    public void beforeClass() throws Exception {
        initMocks(this);

        ScoringCommandLogEntityMgr scoringCommandLogEntityMgr = mock(ScoringCommandLogEntityMgrImpl.class);
        ReflectionTestUtils.setField(scoringCommandLogServiceImpl, "scoringCommandLogEntityMgr", scoringCommandLogEntityMgr);
    }

    @Test(groups = "unit")
    public void testLogBeginStep() { // This test just confirms execution with
                                     // no exceptions raised
        ScoringCommand command = new ScoringCommand("Nutanix", ScoringCommandStatus.POPULATED, "Q_Table_Nutanix", 0, 100, new Timestamp(System.currentTimeMillis()));
        scoringCommandLogServiceImpl.logBeginStep(command, ScoringCommandStep.LOAD_DATA);
        scoringCommandLogServiceImpl.logCompleteStep(command,ScoringCommandStep.LOAD_DATA, ScoringCommandStatus.SUCCESS);
        scoringCommandLogServiceImpl.logLedpException(command, new LedpException(LedpCode.LEDP_18023,
                new IllegalArgumentException("Some test exception message"), new String[] { "sometext" }));

        scoringCommandLogServiceImpl.logException(command, new IllegalArgumentException("Some test exception message"));
        scoringCommandLogServiceImpl.logException(command, "Some message", new IllegalArgumentException(
                "Some test exception message"));
    }
}
