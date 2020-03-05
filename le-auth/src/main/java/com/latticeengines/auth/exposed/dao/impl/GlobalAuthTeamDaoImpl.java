package com.latticeengines.auth.exposed.dao.impl;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.auth.exposed.dao.GlobalAuthTeamDao;
import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.auth.GlobalAuthTeam;

@Component("globalAuthTeamDao")
public class GlobalAuthTeamDaoImpl extends BaseDaoImpl<GlobalAuthTeam> implements GlobalAuthTeamDao {

    @Override
    protected Class<GlobalAuthTeam> getEntityClass() {
        return GlobalAuthTeam.class;
    }

    @Override
    public GlobalAuthTeam findByTeamNameAndTenantId(Long tenantId, String teamName) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTeam> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where name = :teamName and tenantId = %d", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        query.setParameter("teamName", teamName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthTeam)list.get(0);
        }
    }
}
