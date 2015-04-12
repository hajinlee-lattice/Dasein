package com.latticeengines.scoring.dao.impl;


import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.scoring.dao.ScoringCommandResultDao;


public class ScoringCommandResultDaoImpl extends BaseDaoImpl<ScoringCommandResult> implements ScoringCommandResultDao {

    public ScoringCommandResultDaoImpl(){
        super();
    }

    @Override
    protected Class<ScoringCommandResult> getEntityClass() {
        return ScoringCommandResult.class;
    }

    @Override
    public ScoringCommandResult findByScoringCommand(ScoringCommand scoringCommand) {
        Session session = getSessionFactory().getCurrentSession();
        Object result = session
                .createCriteria(ScoringCommandResult.class)
                .add(Restrictions.eq("id", scoringCommand.getId()))
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY).uniqueResult();

        ScoringCommandResult scoringCommandResult = null;
        if (result != null) {
            scoringCommandResult = (ScoringCommandResult)result;
        }

        return scoringCommandResult;

    }
}
