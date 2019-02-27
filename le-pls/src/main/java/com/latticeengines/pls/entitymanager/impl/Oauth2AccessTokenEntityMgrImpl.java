package com.latticeengines.pls.entitymanager.impl;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.Oauth2AccessToken;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.dao.Oauth2AccessTokenDao;
import com.latticeengines.pls.entitymanager.Oauth2AccessTokenEntityMgr;

@Component("oauth2AccessTokenEntityMgr")
public class Oauth2AccessTokenEntityMgrImpl extends BaseEntityMgrImpl<Oauth2AccessToken>
        implements Oauth2AccessTokenEntityMgr {

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
    public Oauth2AccessToken get(String tenantId, String appId) {
        Tenant tenant = tenantEntityMgr.findByTenantId(tenantId);
        if (tenant == null) {
            throw new LedpException(LedpCode.LEDP_18074, new String[] { tenantId });
        }
        Oauth2AccessToken token = findByTenant(tenant, appId);
        if (token != null) {
            token.setAccessToken(CipherUtils.decrypt(token.getAccessToken()));
            return token;
        }
        token = new Oauth2AccessToken();
        token.setTenant(tenant);
        token.setLastModifiedTime(System.currentTimeMillis());
        if (StringUtils.isEmpty(appId)) {
            appId = null;
        }
        token.setAppId(appId);
        token.setAccessToken("");
        return token;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public Oauth2AccessToken findByTenant(Tenant tenant, String appId) {
        return oauth2AccessTokenDao.findByTenantAppId(tenant.getPid(), appId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(Oauth2AccessToken oauth2AccessToken, String tenantId, String appId) {
        Oauth2AccessToken token = get(tenantId, appId);
        token.setAccessToken(CipherUtils.encrypt(oauth2AccessToken.getAccessToken()));
        super.createOrUpdate(token);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void createOrUpdate(Oauth2AccessToken token) {
        super.createOrUpdate(token);
    }

}
