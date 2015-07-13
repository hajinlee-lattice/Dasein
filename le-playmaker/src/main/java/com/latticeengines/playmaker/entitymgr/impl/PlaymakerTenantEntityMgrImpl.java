package com.latticeengines.playmaker.entitymgr.impl;

import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.common.util.RandomValueStringGenerator;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2.db.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.playmaker.dao.PlaymakerTenantDao;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@Component("playmakerTenantEntityMgr")
public class PlaymakerTenantEntityMgrImpl implements PlaymakerTenantEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());

    @Autowired
    private PlaymakerTenantDao tenantDao;

    @Autowired
    private OAuthUserEntityMgr userEngityMgr;

    @Value("${oauth2.password_expiration_days}")
    private int passwordExpirationDays;

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
            user.setPassword(generatePassword());
            user.setPasswordExpiration(getPasswordExpiration(tenant.getTenantName()));
            user.setPasswordExpired(false);
            userEngityMgr.update(user);
        }
        tenant.setTenantPassword(user.getPassword());

        log.info("Created/Updated the following tenantName=" + tenant.getTenantName());
        return tenant;
    }

    private OAuthUser getNewOAuthUser(PlaymakerTenant tenant) {
        OAuthUser user = new OAuthUser();
        user.setUserId(tenant.getTenantName());
        user.setPassword(generatePassword());
        user.setPasswordExpiration(getPasswordExpiration(tenant.getTenantName()));

        return user;
    }

    private String generatePassword() {
        RandomValueStringGenerator generator = new RandomValueStringGenerator(12);
        return generator.generate();
    }

    private Date getPasswordExpiration(String tenantId) {
        if (passwordExpirationDays <= 0) {
            log.info(String.format("oauth2.password_expiration_days <= 0.  Disabling expiration for tenant with id=%s",
                    tenantId));
            return null;
        }
        return DateTime.now(DateTimeZone.UTC).plusDays(passwordExpirationDays).toDate();
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

    @Override
    public String findTenantByTokenId(String tokenId) {
        OAuthUser user = userEngityMgr.getByAccessToken(tokenId);
        if (user != null) {
            return user.getUserId();
        }
        return null;
    }
}
