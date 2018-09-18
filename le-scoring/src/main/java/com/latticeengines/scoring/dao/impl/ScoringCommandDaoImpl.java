package com.latticeengines.scoring.dao.impl;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.dao.ScoringCommandDao;

public class ScoringCommandDaoImpl extends BaseDaoImpl<ScoringCommand> implements ScoringCommandDao {

    public ScoringCommandDaoImpl() {
        super();
    }

    @Override
    protected Class<ScoringCommand> getEntityClass() {
        return ScoringCommand.class;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<ScoringCommand> getPopulated() {
        Session session = getSessionFactory().getCurrentSession();
        List<ScoringCommand> commands = session.createCriteria(ScoringCommand.class) //
                .add(Restrictions.eq("status", ScoringCommandStatus.POPULATED)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .list();
        return commands;
    }

    @SuppressWarnings({ "unchecked", "deprecation" })
    @Override
    public List<ScoringCommand> getConsumed() {
        Session session = getSessionFactory().getCurrentSession();
        List<ScoringCommand> commands = session.createCriteria(ScoringCommand.class) //
                .add(Restrictions.eq("status", ScoringCommandStatus.CONSUMED)) //
                .setResultTransformer(Criteria.DISTINCT_ROOT_ENTITY) //
                .list();
        return commands;
    }

}
