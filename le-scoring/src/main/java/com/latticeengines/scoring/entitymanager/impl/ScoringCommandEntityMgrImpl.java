package com.latticeengines.scoring.entitymanager.impl;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.scoring.dao.ScoringCommandDao;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;

@Component("scoringCommandEntityMgr")
public class ScoringCommandEntityMgrImpl extends BaseScoringEntityMgrImpl<ScoringCommand> implements ScoringCommandEntityMgr{

    public ScoringCommandEntityMgrImpl(){
        super();
    }

    @Autowired
    private ScoringCommandDao scoringCommandDao;

    @Override
    public BaseDao<ScoringCommand> getDao() {
        return scoringCommandDao;
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommand> getPopulated() {
        return scoringCommandDao.getPopulated();
    }

    @Override
    @Transactional(value = "scoring", propagation = Propagation.REQUIRED)
    public List<ScoringCommand> getConsumed() {
        return scoringCommandDao.getConsumed();
    }
}
