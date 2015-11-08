package com.latticeengines.scoring.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.scoring.dao.ScoringCommandLogDao;

public class ScoringCommandLogDaoImpl extends BaseDaoImpl<ScoringCommandLog> implements ScoringCommandLogDao {

    public ScoringCommandLogDaoImpl() {
        super();
    }

    @Override
    protected Class<ScoringCommandLog> getEntityClass() {
        return ScoringCommandLog.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand) {
        Session session = getSessionFactory().getCurrentSession();
        List<ScoringCommandLog> logs = session.createCriteria(ScoringCommandLog.class) //
                .add(Restrictions.eq("scoringCommand", scoringCommand)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .list();
        return logs;
    }

    @Override
    public void delete(ScoringCommand scoringCommand) {
        Session session = getSessionFactory().getCurrentSession();
        String hqlDelete = "delete " + ScoringCommandLog.class.getSimpleName()
                + " scl where scl.scoringCommand = :scoringCommand";
        session.createQuery(hqlDelete).setEntity("scoringCommand", scoringCommand).executeUpdate();
    }
}
