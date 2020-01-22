package com.latticeengines.scoring.entitymanager.impl;

import java.util.List;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.scoring.dao.ScoringCommandResultDao;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;

@Component("scoringCommandResultEntityMgr")
public class ScoringCommandResultEntityMgrImpl extends BaseScoringEntityMgrImpl<ScoringCommandResult> implements ScoringCommandResultEntityMgr{

    @Inject
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
    public ScoringCommandResult findByKey(long pid) {
        return scoringCommandResultDao.findByKey(ScoringCommandResult.class, pid);
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommandResult> getConsumed() {
        return scoringCommandResultDao.getConsumed();
    }

}
