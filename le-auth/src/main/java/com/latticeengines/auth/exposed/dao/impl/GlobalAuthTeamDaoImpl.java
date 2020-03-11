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
                "from %s where name = :teamName and Tenant_Id = %d", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        query.setParameter("teamName", teamName);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthTeam) list.get(0);
        }
    }

    @Override
    public GlobalAuthTeam findByTeamIdAndTenantId(Long tenantId, String teamId) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTeam> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where teamId = :teamId and Tenant_Id = %d", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        query.setParameter("teamId", teamId);
        List list = query.list();
        if (list.size() == 0) {
            return null;
        } else {
            return (GlobalAuthTeam) list.get(0);
        }
    }

    @Override
    public List<GlobalAuthTeam> findByUsernameAndTenantId(Long tenantId, String username) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTeam> entityClz = getEntityClass();
        String queryStr = String.format(
                "select gat from %s as gat join gat.gaUserTenantRights as gaur where gaur.globalAuthUser.email = :username " +
                        "and gat.globalAuthTenant.pid = %d", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        query.setParameter("username", username);
        return query.list();
    }

    @Override
    public List<GlobalAuthTeam> findByTeamIdsAndTenantId(Long tenantId, List<String> teamIds) {
        Session session = sessionFactory.getCurrentSession();
        Class<GlobalAuthTeam> entityClz = getEntityClass();
        String queryStr = String.format(
                "from %s where teamId in (:teamIds) and Tenant_Id = %d ", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(queryStr);
        query.setParameter("teamIds", teamIds);
        return query.list();
    }

    @Override
    public void deleteByTeamId(String teamId, Long tenantId) {
        Session session = getCurrentSession();
        Class<GlobalAuthTeam> entityClz = getEntityClass();
        String deleteStr = String.format(
                "delete from %s where teamId = :teamId and Tenant_Id = %d", entityClz.getSimpleName(), tenantId);
        Query query = session.createQuery(deleteStr);
        query.setParameter("teamId", teamId);
        query.executeUpdate();
    }
}
