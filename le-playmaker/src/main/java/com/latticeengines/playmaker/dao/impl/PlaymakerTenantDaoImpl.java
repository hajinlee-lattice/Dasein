package com.latticeengines.playmaker.dao.impl;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.hibernate.Query;
import org.hibernate.Session;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.dao.PalymakerTenantDao;

public class PlaymakerTenantDaoImpl extends BaseDaoImpl<PlaymakerTenant> implements PalymakerTenantDao {

    @Override
    protected Class<PlaymakerTenant> getEntityClass() {
        return PlaymakerTenant.class;
    }

    @Override
    public PlaymakerTenant findByTenantName(String tenantName) {
        Session session = getSessionFactory().getCurrentSession();
        Class<PlaymakerTenant> entityClz = getEntityClass();

        String queryStr = String.format("from %s where TENANT_NAME = :tenantName", entityClz.getSimpleName());
        Query query = session.createQuery(queryStr);
        query.setString("tenantName", tenantName);
        query.setMaxResults(1);
        @SuppressWarnings("unchecked")
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
        Query query = session.createQuery(queryStr);
        query.setString("tenantName", tenantName);
        int result = query.executeUpdate();
        return result > 0;

    }

    @Override
    public void updateByTenantName(PlaymakerTenant tenant) {
        PlaymakerTenant origTenat = findByTenantName(tenant.getTenantName());
        if (origTenat == null) {
            return;
        }
        origTenat.setExternalId(tenant.getExternalId());
        origTenat.setJdbcDriver(tenant.getJdbcDriver());
        origTenat.setJdbcPassword(tenant.getJdbcPassword());
        origTenat.setJdbcUrl(tenant.getJdbcUrl());
        origTenat.setJdbcUserName(tenant.getJdbcUserName());

        update(origTenat);

    }
}