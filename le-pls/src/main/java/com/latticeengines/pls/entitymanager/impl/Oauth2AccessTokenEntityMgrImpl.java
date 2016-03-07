package com.latticeengines.pls.entitymanager.impl;

import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.Oauth2AccessTokenDao;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.security.exposed.util.SecurityContextUtils;

@Component("oauth2AccessTokenEntityMgr")
public class Oauth2AccessTokenEntityMgrImpl extends BaseEntityMgrImpl<Oauth2AccessToken> implements
        Oauth2AccessTokenEntityMgr {

    @Autowired
    private Oauth2AccessTokenDao oauth2AccessTokenDao;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Override
    public BaseDao<Oauth2AccessToken> getDao() {
        return oauth2AccessTokenDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<Oauth2AccessToken> findAll() {
        return super.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Oauth2AccessToken get() {
        List<Oauth2AccessToken> tokens = super.findAll();
        if (tokens.size() == 1) {
            return tokens.get(0);
        }
        Oauth2AccessToken token = new Oauth2AccessToken();
        Tenant tenant = SecurityContextUtils.getTenant();
        tenant = tenantEntityMgr.findByTenantId(tenant.getId());
        token.setTenant(tenant);
        token.setAccessToken("");
        return token;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(Oauth2AccessToken oauth2AccessToken) {
        Oauth2AccessToken token = get();
        token.setAccessToken(oauth2AccessToken.getAccessToken());
        super.createOrUpdate(token);
    }

}
