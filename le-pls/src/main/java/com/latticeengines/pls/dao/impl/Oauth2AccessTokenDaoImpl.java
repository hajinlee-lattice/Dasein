package com.latticeengines.pls.dao.impl;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.hibernate.Session;
import org.hibernate.query.Query;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.impl.BaseDaoImpl;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.pls.dao.Oauth2AccessTokenDao;

@Component("oauth2AccessTokenDao")
public class Oauth2AccessTokenDaoImpl extends BaseDaoImpl<Oauth2AccessToken> implements Oauth2AccessTokenDao {

    @Override
    protected Class<Oauth2AccessToken> getEntityClass() {
        return Oauth2AccessToken.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Oauth2AccessToken findByTenantAppId(Long tenantId, String appId) {
        Session session = getSessionFactory().getCurrentSession();
        List<Oauth2AccessToken> results = new ArrayList<>();
        if (StringUtils.isEmpty(appId)) {
            String queryStr = String.format("from %s where TENANT_ID = :tenantId and APP_ID IS NULL ",
                    getEntityClass().getSimpleName());
            Query query = session.createQuery(queryStr);
            query.setParameter("tenantId", tenantId);
            results = query.list();
        } else {
            String queryStr = String.format("from %s where TENANT_ID = :tenantId and APP_ID = :appId ",
                    getEntityClass().getSimpleName());
            Query query = session.createQuery(queryStr);
            query.setParameter("tenantId", tenantId);
            query.setParameter("appId", appId);
            results = query.list();
        }
        if (results.size() == 0) {
            return null;
        }
        if (results.size() > 1) {
            throw new RuntimeException("Multiple rows found with tenant " + tenantId + " and appId " + appId);
        }
        return results.get(0);
    }
}
