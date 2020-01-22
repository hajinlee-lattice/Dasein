package com.latticeengines.scoring.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.scoring.dao.ScoringCommandLogDao;
import com.latticeengines.scoring.entitymanager.ScoringCommandLogEntityMgr;

@Component("scoringCommandLogEntityMgr")
public class ScoringCommandLogEntityMgrImpl extends BaseScoringEntityMgrImpl<ScoringCommandLog> implements ScoringCommandLogEntityMgr{

    @Inject
    private ScoringCommandLogDao scoringCommandLogDao;
    
    public ScoringCommandLogEntityMgrImpl(){
        super();
    }
    
    @Override
    public BaseDao<ScoringCommandLog> getDao(){
        return scoringCommandLogDao;
    }

    @Override
    @Transactional(value="scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand) {
        return scoringCommandLogDao.findByScoringCommand(scoringCommand);
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public void delete(ScoringCommand scoringCommand) {
        scoringCommandLogDao.delete(scoringCommand);
    }
}
