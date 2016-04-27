package com.latticeengines.playmaker.entitymgr.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.dao.PlaymakerTenantDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@Component("playmakerTenantEntityMgr")
public class PlaymakerTenantEntityMgrImpl implements PlaymakerTenantEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PlaymakerTenantDao tenantDao;

    @Autowired
    private OAuthUserEntityMgr userEngityMgr;

    @Override
    @Transactional(value = "playmaker")
    public void executeUpdate(PlaymakerTenant tenant) {
        tenantDao.update(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant create(PlaymakerTenant tenant) {

        PlaymakerTenant tenantInDb = tenantDao.findByTenantName(tenant.getTenantName());
        if (tenantInDb == null) {
            tenantDao.create(tenant);
        } else {
            tenantInDb.copyFrom(tenant);
            tenantDao.update(tenantInDb);
        }

        OAuthUser user = null;
        try {
            user = userEngityMgr.get(tenant.getTenantName());
        } catch (Exception ex) {
            log.info("OAuth user does not exist! userId=" + tenant.getTenantName());
        }
        if (user == null) {
            user = getNewOAuthUser(tenant);
            userEngityMgr.create(user);
        } else {
            log.info("Generating new password for tenant " + tenant.getTenantName());
            // Generate new password
            user.setPassword(OAuth2Utils.generatePassword());
            user.setPasswordExpiration(userEngityMgr.getPasswordExpiration(tenant.getTenantName()));
            user.setPasswordExpired(false);
            user.setUserId(tenant.getTenantName());
            userEngityMgr.update(user);
        }
        tenant.setTenantPassword(user.getPassword());

        log.info("Created/Updated the following tenantName=" + tenant.getTenantName());
        return tenant;
    }

    private OAuthUser getNewOAuthUser(PlaymakerTenant tenant) {
        OAuthUser user = new OAuthUser();
        user.setUserId(tenant.getTenantName());
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(userEngityMgr.getPasswordExpiration(tenant.getTenantName()));

        return user;
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByKey(PlaymakerTenant tenant) {
        return tenantDao.findByKey(tenant);
    }

    @Override
    @Transactional(value = "playmaker")
    public PlaymakerTenant findByTenantName(String tenantName) {
        PlaymakerTenant tenant = tenantDao.findByTenantName(tenantName);
        if (tenant != null) {
            OAuthUser user = null;
            try {
                user = userEngityMgr.get(tenantName);
            } catch (Exception ex) {
            }
            if (user != null) {
                tenant.setTenantPassword(user.getPassword());
            } else {
                throw new LedpException(LedpCode.LEDP_22002, new String[] { tenantName });
            }
        }

        return tenant;
    }

    @Override
    @Transactional(value = "playmaker")
    public void deleteByTenantName(String tenantName) {
        tenantDao.deleteByTenantName(tenantName);
        userEngityMgr.delete(tenantName);
        log.info("Deleted the following tenantName=" + tenantName);
    }

    @Override
    @Transactional(value = "playmaker")
    public void updateByTenantName(PlaymakerTenant tenant) {
        tenantDao.updateByTenantName(tenant);
        log.info("Updated the following tenantName=" + tenant.getTenantName());
    }

}
