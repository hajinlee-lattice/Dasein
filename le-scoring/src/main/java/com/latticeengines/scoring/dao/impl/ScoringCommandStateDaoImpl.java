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

    @Override
    public ScoringCommandState findByScoringCommandAndStep(ScoringCommand scoringCommand,
            ScoringCommandStep scoringCommandStep) {
        Session session = getSessionFactory().getCurrentSession();
        Object state = session.createCriteria(ScoringCommandState.class) //
                .add(Restrictions.eq("scoringCommand", scoringCommand)) //
                .add(Restrictions.eq("scoringCommandStep", scoringCommandStep)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .uniqueResult();
        ScoringCommandState scoringCommandState = null;
        if (state != null) {
            scoringCommandState = (ScoringCommandState) state;
        }
        return scoringCommandState;
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

    @Override
    public void delete(ScoringCommand scoringCommand) {
        Session session = getSessionFactory().getCurrentSession();
        String hqlDelete = "delete " + ScoringCommandState.class.getSimpleName()
                + " scs where scs.scoringCommand = :scoringCommand";
        session.createQuery(hqlDelete).setEntity("scoringCommand", scoringCommand).executeUpdate();
    }
}
