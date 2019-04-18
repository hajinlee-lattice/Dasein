package com.latticeengines.oauth2db.dao.impl;

import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.dao.PlaymakerTenantDao;

public class PlaymakerTenantDaoImpl extends BaseDaoImpl<PlaymakerTenant> implements PlaymakerTenantDao {

    @Override
    protected Class<PlaymakerTenant> getEntityClass() {
        return PlaymakerTenant.class;
    }

    @Override
    public PlaymakerTenant findByTenantName(String tenantName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlaymakerTenant> entityClz = getEntityClass();

        String queryStr = String.format("from %s where TENANT_NAME = :tenantName", entityClz.getSimpleName());
        Query<PlaymakerTenant> query = session.createQuery(queryStr, PlaymakerTenant.class);
        query.setParameter("tenantName", tenantName);
        query.setMaxResults(1);
        List<PlaymakerTenant> list = query.list();
        if (!CollectionUtils.isEmpty(list)) {
            return list.get(0);
        } else {
            return null;
        }
    }

    @Override
    public boolean deleteByTenantName(String tenantName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlaymakerTenant> entityClz = getEntityClass();

        String queryStr = String.format("delete from %s where TENANT_NAME = :tenantName", entityClz.getSimpleName());
        Query<?> query = session.createQuery(queryStr);
        query.setParameter("tenantName", tenantName);
        int result = query.executeUpdate();
        return result > 0;

    }

    @Override
    public void updateByTenantName(PlaymakerTenant tenant) {
        PlaymakerTenant origTenant = findByTenantName(tenant.getTenantName());
        if (origTenant == null) {
            return;
        }
        origTenant.setExternalId(tenant.getExternalId());
        origTenant.setJdbcDriver(tenant.getJdbcDriver());
        origTenant.setJdbcUrl(tenant.getJdbcUrl());
        origTenant.setJdbcUserName(tenant.getJdbcUserName());
        origTenant.setJdbcPasswordEncrypt(tenant.getJdbcPasswordEncrypt());
        update(origTenant);

    }
}
