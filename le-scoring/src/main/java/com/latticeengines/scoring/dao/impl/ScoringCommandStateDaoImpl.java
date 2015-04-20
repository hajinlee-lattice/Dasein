package com.latticeengines.scoring.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.dao.ScoringCommandStateDao;

public class ScoringCommandStateDaoImpl extends BaseDaoImpl<ScoringCommandState> implements ScoringCommandStateDao {

    public ScoringCommandStateDaoImpl() {
        super();
    }

    @Override
    protected Class<ScoringCommandState> getEntityClass() {
        return ScoringCommandState.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ScoringCommandState> findByScoringCommandAndStep(ScoringCommand scoringCommand,
            ScoringCommandStep scoringCommandStep) {
        Session session = getSessionFactory().getCurrentSession();
        List<ScoringCommandState> states = session.createCriteria(ScoringCommandState.class) //
                .add(Restrictions.eq("scoringCommand", scoringCommand)) //
                .add(Restrictions.eq("scorinbgCommandStep", scoringCommandStep)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .list();
        return states;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ScoringCommandState> findByScoringCommand(ScoringCommand scoringCommand) {
        Session session = getSessionFactory().getCurrentSession();
        List<ScoringCommandState> states = session.createCriteria(ScoringCommandState.class) //
                .add(Restrictions.eq("scoringCommand", scoringCommand)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .list();
        return states;
    }
}
