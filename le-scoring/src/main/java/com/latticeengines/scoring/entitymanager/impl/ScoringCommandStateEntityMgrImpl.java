package com.latticeengines.scoring.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.dao.ScoringCommandStateDao;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;

@Component("scoringCommandStateEntityMgr")
public class ScoringCommandStateEntityMgrImpl extends BaseScoringEntityMgrImpl<ScoringCommandState> implements
        ScoringCommandStateEntityMgr {

    @Inject
    private ScoringCommandStateDao scoringCommandStateDao;

    public ScoringCommandStateEntityMgrImpl(){
        super();
    }

    @Override
    public BaseDao<ScoringCommandState> getDao() {
        return scoringCommandStateDao;
    }
    
    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommandState> findByScoringCommand(ScoringCommand scoringCommand) {
        return scoringCommandStateDao.findByScoringCommand(scoringCommand);
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public ScoringCommandState findLastStateByScoringCommand(ScoringCommand scoringCommand) {
        List<ScoringCommandState> scoringCommandStates = scoringCommandStateDao.findByScoringCommand(scoringCommand);
        ScoringCommandState scoringCommandState = null;
        if (!scoringCommandStates.isEmpty())
            scoringCommandState = scoringCommandStates.get(scoringCommandStates.size() - 1);
        return scoringCommandState;
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public ScoringCommandState findByScoringCommandAndStep(ScoringCommand scoringCommand,
            ScoringCommandStep scoringCommandStep) {
        return scoringCommandStateDao.findByScoringCommandAndStep(scoringCommand, scoringCommandStep);
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public void delete(ScoringCommand scoringCommand) {
        scoringCommandStateDao.delete(scoringCommand);
    }

}
