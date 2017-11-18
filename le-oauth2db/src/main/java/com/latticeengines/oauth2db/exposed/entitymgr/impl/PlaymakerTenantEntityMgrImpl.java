package com.latticeengines.oauth2db.exposed.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.dao.PlaymakerTenantDao;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;


@Component("playmakerTenantEntityMgr")
public class PlaymakerTenantEntityMgrImpl implements PlaymakerTenantEntityMgr {

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private PlaymakerTenantDao tenantDao;

    @Autowired
    private OAuthUserEntityMgr userEngityMgr;

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void executeUpdate(PlaymakerTenant tenant) {
        tenantDao.update(tenant);
    }

    @Override
    @Transactional(value = "oauth2")
    public PlaymakerTenant create(PlaymakerTenant tenant) {
        encryptPassword(tenant);
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
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlaymakerTenant findByKey(PlaymakerTenant tenant) {
        PlaymakerTenant obj = tenantDao.findByKey(tenant);
        if (obj != null) {
            decryptPassword(obj);
        }
        return obj;
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public PlaymakerTenant findByTenantName(String tenantName) {
        PlaymakerTenant tenant = tenantDao.findByTenantName(tenantName);
        if (tenant != null) {
            decryptPassword(tenant);
            OAuthUser user = null;
            try {
                user = userEngityMgr.get(tenantName);
            } catch (Exception ex) {
                log.warn("Error on getting oauth user " + tenantName, ex.getMessage());
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
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void deleteByTenantName(String tenantName) {
        tenantDao.deleteByTenantName(tenantName);
        userEngityMgr.delete(tenantName);
        log.info("Deleted the following tenantName=" + tenantName);
    }

    @Override
    @Transactional(value = "oauth2", propagation = Propagation.REQUIRED)
    public void updateByTenantName(PlaymakerTenant tenant) {
        encryptPassword(tenant);
        tenantDao.updateByTenantName(tenant);
        log.info("Updated the following tenantName=" + tenant.getTenantName());
    }

    @Transactional(value = "oauth2", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<PlaymakerTenant> findAll() {
        return tenantDao.findAll();
    }

    private void encryptPassword(PlaymakerTenant tenant) {
        if (!StringUtils.isEmpty(tenant.getJdbcPasswordEncrypt())) {
            tenant.setJdbcPasswordEncrypt(CipherUtils.encrypt(tenant.getJdbcPasswordEncrypt()));
        } else if (!StringUtils.isEmpty(tenant.getJdbcPassword())) {
            tenant.setJdbcPasswordEncrypt(CipherUtils.encrypt(tenant.getJdbcPassword()));
        }
    }

    private void decryptPassword(PlaymakerTenant tenant) {
        if (!StringUtils.isEmpty(tenant.getJdbcPasswordEncrypt())) {
            tenant.setJdbcPasswordEncrypt(CipherUtils.decrypt(tenant.getJdbcPasswordEncrypt()));
        }
    }
}
