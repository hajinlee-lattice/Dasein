package com.latticeengines.scoring.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.scoring.dao.ScoringCommandResultDao;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;

@Component("scoringCommandResultEntityMgr")
public class ScoringCommandResultEntityMgrImpl extends BaseScoringEntityMgrImpl<ScoringCommandResult> implements ScoringCommandResultEntityMgr{

    @Autowired
    private ScoringCommandResultDao scoringCommandResultDao;

    public ScoringCommandResultEntityMgrImpl(){
        super();
    }
    
    @Override
    public BaseDao<ScoringCommandResult> getDao(){
        return scoringCommandResultDao;
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public ScoringCommandResult findByScoringCommand(ScoringCommand scoringCommand) {
        return scoringCommandResultDao.findByScoringCommand(scoringCommand);
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommandResult> getConsumed() {
        return scoringCommandResultDao.getConsumed();
    }

}
