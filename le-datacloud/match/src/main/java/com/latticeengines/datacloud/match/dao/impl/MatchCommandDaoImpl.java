package com.latticeengines.datacloud.match.dao.impl;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dao.MatchCommandDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoWithAssignedSessionFactoryImpl;
import com.latticeengines.domain.exposed.datacloud.manage.MatchCommand;

@Component("matchCommandDao")
public class MatchCommandDaoImpl extends BaseDaoWithAssignedSessionFactoryImpl<MatchCommand>
        implements MatchCommandDao {

    @Override
    protected Class<MatchCommand> getEntityClass() {
        return MatchCommand.class;
    }

    @Override
    public void deleteCommand(MatchCommand command) {
        getSessionFactory().getCurrentSession().delete(command);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<MatchCommand> findOutDatedCommands(int retentionDays) {
        Session session = getSessionFactory().getCurrentSession();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, retentionDays*-1);
        Date outDated = cal.getTime();
        String queryStr = String.format("from %s where LatestStatusUpdate < :value", getEntityClass().getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setParameter("value", outDated.toString());
        List<MatchCommand> results = query.list();
        return results;
    }
}
